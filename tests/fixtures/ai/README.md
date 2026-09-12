# AI regression fixtures

These files are byte-for-byte snapshots of the evaluation inputs and model
outputs used by production regression tests when the evaluation suite moved to
[BibleGarden/AI-Evaluation](https://github.com/BibleGarden/AI-Evaluation/tree/main/evaluation).
They are test inputs, not a second editable evaluation dataset. Update a
snapshot only together with the production behavior and the corresponding
evaluation change.

| File | SHA-256 |
| --- | --- |
| `scenarios.json` | `cd1fa940fbcfc427c3fa96824d0b154015f406bd93111c248c6d3e3b7b755cc2` |
| `question_probe_inputs.json` | `beea44cbcfa9a6386a6029db8f1275a7f865e806564ba81415a28da0115bc1c6` |
| `question_quality_inputs.json` | `1f2408250a0e98d6ef589bc15331384ac1e6f68024f48845312787d4a2ef63ec` |
| `bench_data/questions_qwen30b_v1.jsonl` | `671562fe81e9da90de5c96ce78e85c308a97aa99212d4064d4f5731fbf482d85` |
| `bench_data/questions_qwen30b_v3_series.jsonl` | `36f5a35e438defa82cf5a85173b45748bea36e2e2f7b8d4466de113de54fd24a` |
| `question_comparison_2026-09-06/gemini.jsonl` | `b01cddb93e31e8474b361c2263a10ae5765e879dc947d38a5a316fcba3b47259` |
| `question_comparison_2026-09-06/qwen.jsonl` | `745d3fee8f0c073b5e9d070e22aca2624476dcbdd0e473cd0c6dcfb2f25fc375` |
| `question_comparison_prompt_v5_final/gemini.jsonl` | `1e2dd32dddcc360312e058ac9cf3da908edd322b823e88f354085be64b7c1530` |
| `question_comparison_prompt_v5_final/qwen.jsonl` | `09a85b69de7fdf0ac456dcffd0c42e7f705848331ec881d9e425cb180dec33d2` |
