# Sampling of the leading question on Qwen3-30B (ClickUp 86cbejvra)

Prompt **v5** (`--prompt-variant production`), model `qwen3-30b-a3b-instruct-2507`, the eight inputs of `question_quality_inputs_v6a.json`, 3 samples, raw generation — no novelty retry and no safety replacement, exactly as in `question_comparison_prompt_v5_final`. One warm-up call per run is made and recorded separately in `<model>.meta.json` (ADR 0004), so no measured row is a first pass over a new prompt text.

Gender, menus and tails are the verdicts of `app/question_filters.py`; the two loop columns count what the person would see when pressing «заменить вопрос». Sample of 75 answers per configuration — read the differences as directions, not as significance.

| config | sampling | verbatim repeats in series | step-1 collisions (inputs) | wrong gender | gender imposed | menus | tails | mean length | > 160 | distinct texts | `automatic_violations` | median ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `t07` | temperature=0.7 | 4 | 1 (1/8) | 19 | 0 | 9 | 28 | 102 | 3 | 69/75 | 8 | 313 |
| `t07_rerun` | temperature=0.7 | 10 | 1 (1/8) | 15 | 1 | 12 | 21 | 105 | 5 | 64/75 | 7 | 356 |
| `pp08` | presence_penalty=0.8, temperature=0.7 | 10 | 1 (1/8) | 17 | 0 | 9 | 30 | 106 | 1 | 64/75 | 7 | 344 |
| `t10` | temperature=1.0 | 4 | 1 (1/8) | 17 | 1 | 8 | 16 | 104 | 1 | 70/75 | 7 | 351 |
| `t10_minp` | min_p=0.05, temperature=1.0 | 0 | 0 (0/8) | 22 | 0 | 6 | 19 | 106 | 3 | 75/75 | 6 | 350 |
| `t10_minp_rerun` | min_p=0.05, temperature=1.0 | 2 | 1 (1/8) | 16 | 0 | 8 | 23 | 112 | 7 | 72/75 | 13 | 377 |
| _question_comparison_prompt_v5_final (same config, another run)_ | temperature=0.7 (another run) | 6 | 4 (3/8) | 15 | 0 | 10 | 23 | 104 | 1 | 65/75 | 10 | 355 |

## Where the repeats sit

* **t07** — verbatim: {'series-scale-ru': 3, 'series-exhaustion-uk': 1}; step-1 collisions: {'conflict-ru': 1}; violation kinds: {'len160': 3, 'no_advice': 3, 'informal': 2}
* **t07_rerun** — verbatim: {'series-exhaustion-uk': 8, 'series-scale-ru': 2}; step-1 collisions: {'conflict-ru': 1}; violation kinds: {'len160': 5, 'informal': 1, 'no_advice': 1}
* **pp08** — verbatim: {'series-scale-ru': 5, 'series-exhaustion-uk': 5}; step-1 collisions: {'conflict-ru': 1}; violation kinds: {'informal': 3, 'no_advice': 2, 'no_interpret': 1, 'len160': 1}
* **t10** — verbatim: {'series-scale-ru': 2, 'series-exhaustion-uk': 2}; step-1 collisions: {'conflict-ru': 1}; violation kinds: {'informal': 4, 'no_advice': 2, 'len160': 1}
* **t10_minp** — verbatim: none; step-1 collisions: none; violation kinds: {'len160': 3, 'no_advice': 2, 'question': 1, 'no_god_voice': 1, 'informal': 1}
* **t10_minp_rerun** — verbatim: {'series-scale-ru': 1, 'series-exhaustion-uk': 1}; step-1 collisions: {'conflict-ru': 1}; violation kinds: {'len160': 7, 'informal': 3, 'no_advice': 3}
* **question_comparison_prompt_v5_final (same config, another run)** — verbatim: {'series-exhaustion-uk': 6}; step-1 collisions: {'joy-ru': 2, 'series-gratitude-ru': 1, 'conflict-ru': 1}; violation kinds: {'informal': 6, 'no_interpret': 3, 'len160': 1, 'no_advice': 1}

Reproduce (the key is read into the shell and never written to a file):

```bash
cd /root/cep/Bible-API/evaluation
export AI_OPENAI_COMPAT_API_KEY="$(docker exec bible-api sh -c 'printf %s "$AI_OPENAI_COMPAT_API_KEY"')"
python3 compare_question_models.py run --models qwen --inputs question_quality_inputs_v6a.json --samples 3 --timeout 60 \
  --out bench_data/question_v6a_sampling/t07
python3 compare_question_models.py run --models qwen --inputs question_quality_inputs_v6a.json --samples 3 --timeout 60 \
  --out bench_data/question_v6a_sampling/t07_rerun
python3 compare_question_models.py run --models qwen --inputs question_quality_inputs_v6a.json --samples 3 --timeout 60 --presence-penalty 0.8 --temperature 0.7 \
  --out bench_data/question_v6a_sampling/pp08
python3 compare_question_models.py run --models qwen --inputs question_quality_inputs_v6a.json --samples 3 --timeout 60 --temperature 1.0 \
  --out bench_data/question_v6a_sampling/t10
python3 compare_question_models.py run --models qwen --inputs question_quality_inputs_v6a.json --samples 3 --timeout 60 --min-p 0.05 --temperature 1.0 \
  --out bench_data/question_v6a_sampling/t10_minp
python3 compare_question_models.py run --models qwen --inputs question_quality_inputs_v6a.json --samples 3 --timeout 60 --min-p 0.05 --temperature 1.0 \
  --out bench_data/question_v6a_sampling/t10_minp_rerun
python3 question_v6a_report.py
```
