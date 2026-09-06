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
