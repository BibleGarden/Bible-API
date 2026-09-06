#!/usr/bin/env python3
"""Reproducible raw-model comparison and offline blind review (86cbejhh9).

Uses the existing generator's exact prompts/transports. No application config
is changed. Secrets are read from named environment variables, never persisted.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import statistics
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import httpx

import gen_questions as gen
from check_questions import check, violations

HERE = Path(__file__).resolve().parent


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def write_json(path, value):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def load_models(path):
    models = read_json(path)
    for name, model in models.items():
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", name):
            raise ValueError("Model aliases must be lowercase filename-safe identifiers")
        if set(model) - {"provider", "model", "endpoint", "api_key_env", "pause_seconds"}:
            raise ValueError(f"{name}: unknown setting (store keys in environment only)")
        if model.get("provider") not in {"gemini", "openai_compat"}:
            raise ValueError(f"{name}: unsupported provider")
        if not re.fullmatch(r"[A-Za-z0-9_./:-]+", model.get("model", "")):
            raise ValueError(f"{name}: missing or invalid model identifier")
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", model.get("api_key_env", "")):
            raise ValueError(f"{name}: api_key_env must name an environment variable")
        if model.get("pause_seconds", 0) < 0:
            raise ValueError(f"{name}: pause_seconds must not be negative")
        if model["provider"] == "openai_compat":
            parts = urlsplit(model.get("endpoint", ""))
            if parts.scheme not in {"https", "http"} or not parts.hostname or parts.username or parts.password or parts.query or parts.fragment:
                raise ValueError(f"{name}: endpoint must be an HTTP(S) URL without credentials or query")
    return models


def sampling_args(args):
    """The sampling flags of this run as the namespace `gen.sampling` reads.

    One place builds them, so the payload, `protocol.json` and every model's
    `meta.json` cannot disagree about what was sent (ClickUp 86cbejvra).
    """
    values = argparse.Namespace(
        temperature=getattr(args, 'temperature', None),
        top_p=getattr(args, 'top_p', None), top_k=getattr(args, 'top_k', None),
        presence_penalty=getattr(args, 'presence_penalty', None),
        min_p=getattr(args, 'min_p', None))
    for field, low, high in (('temperature', 0., 2.), ('presence_penalty', -2., 2.),
                             ('min_p', 0., 1.), ('top_p', 0., 1.)):
        value = getattr(values, field)
        if value is not None and not low <= value <= high:
            raise ValueError(f'{field} must be between {low} and {high}')
    return values


def protocol(inputs_path, samples, timeout=30, variant="production", overrides=None):
    if samples < 1 or timeout <= 0:
        raise ValueError("samples and timeout must be positive")
    overrides = dict(overrides or {})
    entries = gen.load_series(inputs_path)
    if not entries or len({e['id'] for e in entries}) != len(entries):
        raise ValueError("Input ids must be nonempty and unique")
    if any(e['steps'] > gen.MAX_SKIPPED_QUESTIONS + 1 for e in entries):
        raise ValueError("Series exceeds skipped_questions capacity")
    # Capture exact prompts, including skipped-block wording, so an added model
    # cannot silently be compared against old answers after a prompt edit.
    prompts = {lang: gen.question_prompts.system_prompt(variant, lang) for lang in ('ru', 'uk', 'en')}
    prompts['universal'] = gen.question_prompts.system_prompt(variant, None)
    builders = {e['id']: [gen.user_message(e, variant=variant), gen.user_message(e, ['EXAMPLE_SKIPPED_QUESTION'], variant)] for e in entries}
    spec = {'version': 1, 'inputs': read_json(inputs_path), 'samples': samples,
            'system_prompts': prompts, 'user_prompts': builders,
            'temperature': overrides.get('temperature', gen.TEMPERATURE),
            'max_output_tokens': gen.MAX_OUTPUT_TOKENS,
            'mode': 'raw generation, accumulated skips, no novelty retry or safety replacement',
            'transport_attempts': 1, 'timeout_seconds': timeout, 'prompt_variant': variant}
    # Only a run that overrides sampling carries the key, so a default run
    # hashes exactly as it did before 86cbejvra and a model can still be added
    # to a directory measured earlier. A run WITH overrides hashes differently
    # and is therefore pushed into its own directory by the protocol guard —
    # which is the point: two sampling configurations are two comparisons.
    if overrides:
        spec['sampling_overrides'] = overrides
    return entries, spec


def run(args):
    original_attempts = gen.TRANSPORT_ATTEMPTS
    try:
        return _run(args)
    finally:
        gen.TRANSPORT_ATTEMPTS = original_attempts


def _run(args):
    models = load_models(args.config)
    unknown = set(args.models) - set(models)
    if unknown:
        raise ValueError(f"Unknown aliases: {sorted(unknown)}")
    if len(args.models) != len(set(args.models)):
        raise ValueError('Model aliases must not be repeated')
    variant = getattr(args, 'prompt_variant', 'production')
    call_defaults = sampling_args(args)
    overrides = gen.sampling(call_defaults)
    if overrides and any(models[name]['provider'] == 'gemini' for name in args.models):
        # `gen.call_gemini` sends the production generationConfig and ignores
        # this dict, so a mixed run would report an override it never applied.
        raise ValueError('Sampling overrides are openai_compat-only: run gemini separately')
    entries, spec = protocol(args.inputs, args.samples, args.timeout, variant, overrides)
    identity = digest(spec)
    expected = sum(e['steps'] for e in entries) * args.samples
    if args.dry_run:
        print(json.dumps({'models': {k:models[k] for k in args.models}, 'per_model_calls': expected,
                          'warmup_calls_per_model': 1, 'protocol_hash': identity}, ensure_ascii=False, indent=2))
        return 0
    # Fail before any billable calls when one selected model has no credential.
    for name in args.models:
        if not os.environ.get(models[name]['api_key_env']):
            raise ValueError(f"{name}: set {models[name]['api_key_env']}")
    args.out.mkdir(parents=True, exist_ok=True)
    spec_path = args.out / 'protocol.json'
    if spec_path.exists() and read_json(spec_path) != spec:
        raise ValueError("Protocol changed: use a new output directory for a new comparison")
    for name in args.models:
        if (args.out / f'{name}.jsonl').exists() or (args.out / f'{name}.meta.json').exists():
            raise ValueError(f'{name}: run already exists; preserve it and use a new directory')
    write_json(spec_path, spec)
    gen.TRANSPORT_ATTEMPTS = 1
    failed = False
    for name in args.models:
        model = models[name]
        path = args.out / f'{name}.jsonl'
        meta_path = args.out / f'{name}.meta.json'
        if path.exists() or meta_path.exists():
            # Never silently overwrite or refill a partial measured run.
            raise ValueError(f"{name}: run already exists; preserve it and use a new directory")
        meta = {'alias': name, 'model_config': model, 'protocol_hash': identity,
                'started_at': datetime.now(timezone.utc).isoformat(), 'expected': expected,
                # What was actually in the payload beside the production pair —
                # empty for every run before 86cbejvra and for every default one.
                'sampling_overrides': overrides,
                'temperature': spec['temperature'],
                'written': 0, 'failed': 0, 'complete': False}
        write_json(meta_path, meta)
        call_args = argparse.Namespace(provider='gemini' if model['provider']=='gemini' else 'qwen',
            prompt_variant=variant, candidates=1, retry_on_repeat=False,
            **vars(call_defaults))
        url = (gen.GEMINI_URL_TEMPLATE.format(model=model['model']) if model['provider']=='gemini'
               else model['endpoint'].rstrip('/') + '/chat/completions')
        key = os.environ[model['api_key_env']]
        with httpx.Client(timeout=args.timeout) as client:
            # One separately recorded warm-up, not a claim every prompt is warm.
            warmup = gen.generate_one(client, call_args, url, key, model['model'], entries[0], 0)
            meta['warmup'] = warmup
            write_json(meta_path, meta)
            if warmup['error']:
                print(f"{name}: warm-up failed: {warmup['error']}", flush=True)
                failed = True
                continue
            time.sleep(model.get('pause_seconds', 0))
            for entry in entries:
                for sample in range(1, args.samples + 1):
                    skipped = []
                    for step in range(1, entry['steps'] + 1):
                        record = gen.generate_one(client, call_args, url, key, model['model'], entry, sample,
                                                  step=step, skipped_questions=list(skipped))
                        record['step'] = step
                        record['input'] = {k:entry[k] for k in ('topic','stage','messages')}
                        record['sent_user_message'] = gen.user_message(entry, skipped, variant)
                        record['sent_system_prompt'] = gen.question_prompts.system_prompt(variant, record.get('prompt_language'))
                        record['automatic_violations'] = violations(check(record))
                        record['skipped_questions'] = list(skipped)
                        gen.append_record(path, record)
                        meta['written'] += 1
                        meta['failed'] += bool(record['error'])
                        write_json(meta_path, meta)
                        print(f"{name}: {meta['written']}/{expected} {entry['id']} {sample}.{step} "
                              f"{'ERROR' if record['error'] else 'ok'}", flush=True)
                        if record['error']:
                            # No biased surviving subset and no hundreds of quota retries.
                            return 1
                        skipped.append(record['text'])
                        time.sleep(model.get('pause_seconds', 0))
        meta['complete'] = meta['written'] == expected and not meta['failed']
        meta['finished_at'] = datetime.now(timezone.utc).isoformat()
        write_json(meta_path, meta)
    return int(failed)


def load_runs(directory):
    spec = read_json(directory / 'protocol.json')
    runs = {}
    artifacts = {path.stem for path in directory.glob('*.jsonl')}
    metadata = {path.name.removesuffix('.meta.json') for path in directory.glob('*.meta.json')}
    if artifacts != metadata:
        raise ValueError('Incomplete run: each model needs both metadata and an answer artifact')
    expected_keys = {(e['id'], sample, step)
                     for e in spec['inputs']['inputs']
                     for sample in range(1, spec['samples'] + 1)
                     for step in range(1, (e.get('replacements', 1) if e.get('kind') == 'series' else 1) + 1)}
    for path in sorted(directory.glob('*.jsonl')):
        meta = read_json(directory / f'{path.stem}.meta.json')
        rows = [json.loads(s) for s in path.read_text(encoding='utf-8').splitlines()]
        keys = [(r['id'],r['sample'],r['step']) for r in rows]
        if (meta['protocol_hash'] != digest(spec) or not meta['complete'] or
            len(keys) != len(expected_keys) or set(keys) != expected_keys or any(r['error'] or r['model'] != meta['model_config']['model'] for r in rows)):
            raise ValueError(f'{path.stem}: incomplete or incompatible run; cannot rank a surviving subset')
        runs[path.stem] = (meta, rows)
    if len(runs) < 2:
        raise ValueError('A comparison needs at least two complete model runs')
    return spec, runs


def review_data(directory, seed):
    spec, runs = load_runs(directory)
    grouped = {}
    for alias, (meta, rows) in runs.items():
        for row in sorted(rows, key=lambda r: (r['id'], r['sample'], r['step'])):
            key = (row['id'], row['sample'])
            card = grouped.setdefault(key, {'id': f'{key[0]}:{key[1]}', 'context': row['input'], 'options': {}})
            card['options'].setdefault(alias, []).append(row['text'])
    rng = random.Random(seed)
    cards = []
    for card in grouped.values():
        aliases = list(card['options'])
        rng.shuffle(aliases)
        card['options'] = [{'alias': alias, 'model': runs[alias][0]['model_config']['model'],
                            'texts': card['options'][alias]} for alias in aliases]
        cards.append(card)
    rng.shuffle(cards)
    identity = digest({'protocol':spec, 'seed':seed, 'runs':{a: r for a,(_,r) in runs.items()}})
    return {'id':identity, 'seed':seed, 'cards':cards}, runs


def report(args):
    data, runs = review_data(args.out, args.seed)
    template = (HERE / 'question_review.html').read_text(encoding='utf-8')
    # Escape script termination and JS line separators in untrusted model text.
    payload = json.dumps(data, ensure_ascii=False).replace('<','\\u003c').replace('\u2028','\\u2028').replace('\u2029','\\u2029')
    (args.out / 'review.html').write_text(template.replace('__REVIEW_DATA__', payload), encoding='utf-8')
    spec = read_json(args.out / 'protocol.json')
    prompt_data = {
        'system_prompts': spec['system_prompts'],
        'temperature': spec['temperature'],
        'max_output_tokens': spec['max_output_tokens'],
        'runs': {alias: {'model': meta['model_config']['model'], 'records': [
            {key: row[key] for key in ('id', 'sample', 'step', 'text', 'prompt_language',
                                      'sent_user_message', 'latency_ms')} | {
                'topic': row['input']['topic'],
                'sent_system_prompt': row.get('sent_system_prompt', spec['system_prompts'].get(row['prompt_language'] or 'universal', ''))}
            for row in rows]} for alias, (meta, rows) in runs.items()},
    }
    prompt_template = (HERE / 'question_prompt_view.html').read_text(encoding='utf-8')
    prompt_json = json.dumps(prompt_data, ensure_ascii=False).replace('<', '\\u003c')
    (args.out / 'prompts.html').write_text(
        prompt_template.replace('__PROMPT_DATA__', prompt_json), encoding='utf-8')
    sampling_note = (f" Sampling: temperature {spec['temperature']}"
                     + (f", overrides {json.dumps(spec['sampling_overrides'], ensure_ascii=False)}."
                        if spec.get('sampling_overrides') else ' (production payload).'))
    lines = ['# Question model comparison', '', 'Raw generation; exact prompts are in protocol.json. No selection of best samples. '
             'Formal checks are heuristics, not a judgment of depth or usefulness.' + sampling_note, '',
             '| Model | Answers | Median / p90 ms | Answers with heuristic flags | Exact / near repeats against shown |', '|---|---:|---:|---:|---:|']
    for alias, (meta, rows) in runs.items():
        latencies = sorted(r['latency_ms'] for r in rows)
        p90 = latencies[min(len(latencies)-1, int(.9*(len(latencies)-1)))]
        flagged = sum(bool(r['automatic_violations']) for r in rows)
        repeats = Counter(gen.is_repeat(r['text'], [m['text'] for m in r['input']['messages'] if m['role']=='assistant'] + r['skipped_questions']).kind for r in rows)
        lines.append(f"| {meta['model_config']['model']} | {len(rows)} | {statistics.median(latencies):.0f} / {p90} | {flagged} | {repeats['exact']} / {repeats['near']} |")
    for alias, (_, rows) in runs.items():
        counts = Counter(v for r in rows for v in r['automatic_violations'])
        lines.extend(['', f'## {alias}', '', json.dumps(counts, ensure_ascii=False), ''])
        for row in rows:
            lines.extend([f"### {row['id']} · sample {row['sample']} · step {row['step']}", '', row['text'], ''])
    (args.out / 'report.md').write_text('\n'.join(lines), encoding='utf-8')
    print(f"Written {args.out / 'review.html'} and report.md / prompts.html ({len(data['cards'])} blind cards)")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['run','report'])
    parser.add_argument('--config', type=Path, default=HERE/'question_models.json')
    parser.add_argument('--inputs', type=Path, default=HERE/'question_quality_inputs.json')
    parser.add_argument('--models', nargs='+', default=['qwen','gemini'])
    parser.add_argument('--samples', type=int, default=3)
    parser.add_argument('--prompt-variant', default='production',
                        choices=('production', *gen.question_prompts.VARIANTS))
    # Sampling levers (ClickUp 86cbejvra). Unset = the production payload, so
    # an omitted flag changes neither the request nor the protocol hash.
    parser.add_argument('--temperature', type=float, default=None,
                        help=f'override the production temperature ({gen.TEMPERATURE}); '
                             'openai_compat models only')
    parser.add_argument('--presence-penalty', type=float, default=None,
                        help='OpenAI-compatible presence_penalty; openai_compat models only')
    parser.add_argument('--min-p', type=float, default=None,
                        help="vLLM's min_p; openai_compat models only")
    parser.add_argument('--timeout', type=float, default=30)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--seed', type=int, default=86)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)
    try:
        return run(args) if args.action=='run' else report(args)
    except (ValueError, KeyError, OSError) as error:
        # Config errors only; provider errors are sanitized by gen_questions.
        print(f'Comparison failed: {error}')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
