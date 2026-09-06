"""The comparison must preserve every sample and never mix protocols."""
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

EVALUATION = Path(__file__).resolve().parents[1] / 'evaluation'
sys.path.insert(0, str(EVALUATION))
spec = importlib.util.spec_from_file_location('comparison', EVALUATION / 'compare_question_models.py')
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)


def inputs(tmp_path):
    path = tmp_path / 'inputs.json'
    comparison.write_json(path, {'inputs': [
        {'id':'one','kind':'single','language':'ru','category':'quality','topic':'Радость',
         'stage':'first','messages':[]},
        {'id':'series','kind':'series','replacements':2,'language':'ru','category':'quality',
         'topic':'Радость','stage':'next','messages':[{'role':'user','text':'Я рада встрече.'}]},
    ]})
    return path


def setup_run(tmp_path, monkeypatch):
    config = tmp_path / 'models.json'
    comparison.write_json(config, {
        'a': {'provider':'openai_compat','model':'model-a','endpoint':'https://example.org/v1','api_key_env':'TEST_KEY'},
        'b': {'provider':'gemini','model':'model-b','api_key_env':'TEST_KEY'},
    })
    monkeypatch.setenv('TEST_KEY','secret-never-in-artifact')
    monkeypatch.setattr(comparison.time, 'sleep', lambda _: None)
    args = SimpleNamespace(config=config, inputs=inputs(tmp_path), models=['a','b'],
                           samples=2, out=tmp_path/'out', dry_run=False, timeout=30, seed=12)
    return args


def fake_generation(client, args, url, key, model, entry, sample, step=1, skipped_questions=None):
    return {'id':entry['id'],'sample':sample,'text':f'Вопрос {model} {sample} {step}?',
            'latency_ms':50,'error':None,'language':'ru','prompt_language':'ru','model':model}


def test_run_uses_same_protocol_and_keeps_all_samples(tmp_path, monkeypatch):
    args = setup_run(tmp_path, monkeypatch)
    calls=[]
    def generate(*a, **kw):
        calls.append((a[4],a[5]['id'],a[6],kw.get('step',1),kw.get('skipped_questions')))
        return fake_generation(*a, **kw)
    monkeypatch.setattr(comparison.gen, 'generate_one', generate)
    assert comparison.run(args)==0
    spec, runs = comparison.load_runs(args.out)
    assert len(runs)==2
    for meta, rows in runs.values():
        assert meta['expected']==6 and len(rows)==6 and meta['complete']
        assert meta['warmup']['sample']==0
        assert all(r['sample']>0 for r in rows)
        for r in rows:
            assert len(r['skipped_questions'])==r['step']-1
    assert calls[1][1:]==calls[8][1:]
    assert 'secret-never-in-artifact' not in ''.join(p.read_text() for p in args.out.iterdir())
    data,_=comparison.review_data(args.out,12)
    assert len(data['cards'])==4
    assert sum(len(o['texts']) for c in data['cards'] for o in c['options'])==12
    assert data==comparison.review_data(args.out,12)[0]
    assert comparison.report(args)==0
    assert '__REVIEW_DATA__' not in (args.out/'review.html').read_text()


def test_add_model_without_regenerating_old_one(tmp_path, monkeypatch):
    args=setup_run(tmp_path,monkeypatch)
    monkeypatch.setattr(comparison.gen,'generate_one',fake_generation)
    args.models=['a'];assert comparison.run(args)==0
    before=(args.out/'a.jsonl').read_bytes()
    args.models=['b'];assert comparison.run(args)==0
    assert (args.out/'a.jsonl').read_bytes()==before
    assert len(comparison.load_runs(args.out)[1])==2
    with pytest.raises(ValueError,match='already exists'):
        comparison.run(args)


def test_changed_protocol_and_failed_runs_are_not_compared(tmp_path, monkeypatch):
    args=setup_run(tmp_path,monkeypatch)
    monkeypatch.setattr(comparison.gen,'generate_one',fake_generation)
    assert comparison.run(args)==0
    args.samples=3
    with pytest.raises(ValueError,match='Protocol changed'):
        comparison.run(args)
    path=args.out/'a.jsonl'
    rows=path.read_text().splitlines();path.write_text('\n'.join(rows[:-1])+'\n')
    with pytest.raises(ValueError,match='incomplete'):
        comparison.load_runs(args.out)


def test_missing_key_fails_before_any_calls(tmp_path,monkeypatch):
    args=setup_run(tmp_path,monkeypatch);monkeypatch.delenv('TEST_KEY')
    monkeypatch.setattr(comparison.gen,'generate_one',lambda *a,**k:pytest.fail('Network call'))
    with pytest.raises(ValueError,match='set TEST_KEY'):
        comparison.run(args)
    assert not args.out.exists()


def test_report_cannot_hide_third_model_with_failed_warmup(tmp_path, monkeypatch):
    args = setup_run(tmp_path, monkeypatch)
    monkeypatch.setattr(comparison.gen, 'generate_one', fake_generation)
    assert comparison.run(args) == 0
    comparison.write_json(args.out / 'third.meta.json', {'complete': False})
    with pytest.raises(ValueError, match='Incomplete run'):
        comparison.load_runs(args.out)


def test_failed_provider_does_not_produce_successful_comparison(tmp_path,monkeypatch):
    args=setup_run(tmp_path,monkeypatch)
    def fail(*a,**kw):
        record=fake_generation(*a,**kw)
        if a[6]>0:record.update(text='',error='transport: HTTPStatusError (HTTP 429)')
        return record
    monkeypatch.setattr(comparison.gen,'generate_one',fail)
    assert comparison.run(args)==1
    meta=comparison.read_json(args.out/'a.meta.json')
    assert not meta['complete'] and meta['failed']==1
    with pytest.raises(ValueError,match='incomplete'):
        comparison.load_runs(args.out)


def test_config_rejects_credentials_in_url(tmp_path):
    path=tmp_path/'models.json'
    for url in ('https://user:secret@example.org/v1','https://example.org/v1?key=secret'):
        comparison.write_json(path,{'a':{'provider':'openai_compat','model':'x','endpoint':url,'api_key_env':'KEY'}})
        with pytest.raises(ValueError,match='without credentials'):
            comparison.load_models(path)


def test_html_cannot_execute_model_output(tmp_path,monkeypatch):
    args=setup_run(tmp_path,monkeypatch)
    def injected(*a,**kw):
        r=fake_generation(*a,**kw);r['text']='</script><script>alert(1)</script>?';return r
    monkeypatch.setattr(comparison.gen,'generate_one',injected)
    comparison.run(args);comparison.report(args)
    html=(args.out/'review.html').read_text()
    assert '</script><script>alert(1)' not in html
    assert '\\u003c/script>' in html


def test_prompt_variant_is_part_of_protocol(tmp_path):
    path = inputs(tmp_path)
    _, baseline = comparison.protocol(path, 1, variant='v4')
    _, localized = comparison.protocol(path, 1, variant='production')
    assert baseline['prompt_variant'] == 'v4'
    assert baseline['system_prompts']['ru'] != localized['system_prompts']['ru']
    assert 'universal' in localized['system_prompts']
    assert comparison.digest(baseline) != comparison.digest(localized)


def test_before_after_checks_inputs_and_keeps_all_variants(tmp_path, monkeypatch):
    import compare_question_prompts as prompt_comparison
    args = setup_run(tmp_path, monkeypatch)
    monkeypatch.setattr(comparison.gen, 'generate_one', fake_generation)
    directories = []
    for variant in ('v4', 'production'):
        args.out = tmp_path / variant
        args.prompt_variant = variant
        assert comparison.run(args) == 0
        directories.append(args.out)
    prompt_comparison.build(directories, tmp_path / 'review')
    html = (tmp_path / 'review' / 'review.html').read_text()
    assert 'production' in html and 'v4' in html
    assert 'Вопрос model-a' in html
    # Change protocol consistently so load_runs passes but comparison refuses.
    spec_path = directories[-1] / 'protocol.json'
    spec = comparison.read_json(spec_path)
    spec['temperature'] = .1
    comparison.write_json(spec_path, spec)
    for model in ('a', 'b'):
        meta_path = directories[-1] / f'{model}.meta.json'
        meta = comparison.read_json(meta_path)
        meta['protocol_hash'] = comparison.digest(spec)
        comparison.write_json(meta_path, meta)
    with pytest.raises(ValueError, match='identical inputs'):
        prompt_comparison.build(directories, tmp_path / 'bad')


def test_sampling_overrides_are_recorded_and_force_a_new_directory(tmp_path, monkeypatch):
    """ClickUp 86cbejvra: what was sent must be readable from the artifacts.

    Three things at once, because they are one guarantee: the flags reach the
    namespace `gen_questions` builds its payload from, they land in
    `protocol.json` and in every model's `meta.json`, and a run that changes
    them cannot be mixed into a directory measured without them.
    """
    seen = []

    def generate(*a, **kw):
        seen.append(a[1])  # the call namespace
        return fake_generation(*a, **kw)

    monkeypatch.setattr(comparison.gen, 'generate_one', generate)
    args = setup_run(tmp_path, monkeypatch)
    args.models = ['a']
    args.temperature, args.presence_penalty, args.min_p = 1.0, .8, .05
    assert comparison.run(args) == 0

    assert seen and all(
        (call.temperature, call.presence_penalty, call.min_p) == (1.0, .8, .05)
        for call in seen
    )
    spec = comparison.read_json(args.out / 'protocol.json')
    assert spec['temperature'] == 1.0
    assert spec['sampling_overrides'] == {
        'temperature': 1.0, 'presence_penalty': .8, 'min_p': .05}
    meta = comparison.read_json(args.out / 'a.meta.json')
    assert meta['sampling_overrides'] == spec['sampling_overrides']
    assert meta['temperature'] == 1.0
    # The same directory with different sampling is a different comparison.
    args.min_p = None
    with pytest.raises(ValueError, match='Protocol changed'):
        comparison.run(args)


def test_a_default_run_hashes_exactly_as_before_the_flags_existed(tmp_path):
    """No flag = no key in the protocol, so an old directory still accepts a model."""
    path = inputs(tmp_path)
    _, plain = comparison.protocol(path, 1)
    _, also_plain = comparison.protocol(path, 1, overrides={})
    _, hot = comparison.protocol(path, 1, overrides={'temperature': 1.0})
    assert 'sampling_overrides' not in plain and plain['temperature'] == comparison.gen.TEMPERATURE
    assert comparison.digest(plain) == comparison.digest(also_plain)
    assert comparison.digest(plain) != comparison.digest(hot)


def test_sampling_overrides_are_refused_for_gemini_and_out_of_range(tmp_path, monkeypatch):
    args = setup_run(tmp_path, monkeypatch)
    monkeypatch.setattr(comparison.gen, 'generate_one',
                        lambda *a, **k: pytest.fail('Network call'))
    args.temperature = 1.0
    # 'b' is the gemini model: its transport ignores this dict entirely.
    with pytest.raises(ValueError, match='openai_compat-only'):
        comparison.run(args)
    args.models, args.temperature, args.min_p = ['a'], None, 5.
    with pytest.raises(ValueError, match='min_p must be between'):
        comparison.run(args)
