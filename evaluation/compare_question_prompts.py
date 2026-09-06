#!/usr/bin/env python3
"""Build an offline before/after prompt review from complete model runs."""
import argparse
import json
import random
from pathlib import Path

from compare_question_models import HERE, digest, load_runs, read_json


def build(directories, output, seed=86):
    runs = {}
    common = None
    for directory in directories:
        spec, models = load_runs(directory)
        invariant = {k: spec[k] for k in ('inputs', 'samples', 'temperature', 'max_output_tokens', 'mode', 'transport_attempts', 'timeout_seconds')}
        if common is not None and invariant != common:
            raise ValueError('Before/after runs need identical inputs, sample count and generation settings')
        common = invariant
        variant = spec.get('prompt_variant', 'v4')
        if variant in runs:
            raise ValueError('Prompt variant names must be unique')
        runs[variant] = (spec, models)
    if len(runs) < 2:
        raise ValueError('Provide at least two prompt variants')
    names = None
    for spec, models in runs.values():
        identities = {alias: meta['model_config'] for alias, (meta, rows) in models.items()}
        if names is not None and identities != names:
            raise ValueError('Models and provider configuration must match across prompt variants')
        names = identities
    cards = {}
    inspect = {}
    for variant, (spec, models) in runs.items():
        for alias, (meta, rows) in models.items():
            option_id = alias + '-' + variant
            variant_label = {'production':'v5 — локализованный', 'v5-structured':'v5 — английские инструкции', 'v4':'v4 — исходный'}.get(variant, variant)
            label = meta['model_config']['model'] + ' · ' + variant_label
            inspect[option_id] = {'model': label, 'records': []}
            for row in sorted(rows, key=lambda r: (r['id'], r['sample'], r['step'])):
                system = row.get('sent_system_prompt') or spec['system_prompts'][row['prompt_language'] or 'universal']
                record = {k: row[k] for k in ('id','sample','step','text','prompt_language','sent_user_message','latency_ms')}
                record.update(topic=row['input']['topic'], sent_system_prompt=system)
                inspect[option_id]['records'].append(record)
                key = f"{alias}:{row['id']}:{row['sample']}"
                card = cards.setdefault(key, {'id':key, 'model':meta['model_config']['model'], 'context':row['input'], 'options':{}})
                option = card['options'].setdefault(variant, {'alias':variant, 'model':variant_label, 'texts':[]})
                option['texts'].append(row['text'])
    rng = random.Random(seed)
    result = list(cards.values())
    for card in result:
        card['options'] = list(card['options'].values())
        rng.shuffle(card['options'])
    rng.shuffle(result)
    data = {'id':digest({'runs':inspect,'protocol':common,'seed':seed}), 'kind':'prompts', 'seed':seed, 'cards':result}
    output.mkdir(parents=True, exist_ok=True)
    def render(template, placeholder, value, filename):
        text = json.dumps(value, ensure_ascii=False).replace('<','\\u003c')
        (output/filename).write_text((HERE/template).read_text().replace(placeholder,text),encoding='utf-8')
    render('question_review.html','__REVIEW_DATA__',data,'review.html')
    render('question_prompt_view.html','__PROMPT_DATA__',{
        'system_prompts':{},'temperature':common['temperature'],
        'max_output_tokens':common['max_output_tokens'],'runs':inspect},'prompts.html')
    (output/'sources.json').write_text(json.dumps({str(d):digest(read_json(d/'protocol.json')) for d in directories},indent=2)+'\n')
    print(f'{len(result)} cards written to {output}; all prompt variants retained')


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories',nargs='+',type=Path)
    parser.add_argument('--out',required=True,type=Path)
    parser.add_argument('--seed',default=86,type=int)
    args=parser.parse_args()
    build(args.directories,args.out,args.seed)
