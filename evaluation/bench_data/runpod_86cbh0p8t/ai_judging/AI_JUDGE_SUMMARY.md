# Preliminary AI judge summary

Prepared on 2026-09-12 for ClickUp 86cbh0p8t. This is a diagnostic comparison
of Qwen (`run_a`) and Gemma (`run_b`) on the synthetic question fixture. It is
not Maria's qualitative acceptance and does not authorize a model change.

The comparison contains 52 elementary main pairs from 20 inputs and two
retained samples. Series steps from one input are correlated. Each pair was
shown in both orientations. Five additional controls compare two different
Qwen samples for the same input and step; they are not identical-answer controls
with a ground-truth tie.

## Results

| Judge | Complete main pairs | Qwen | Gemma | Tie | Orientation agreement |
| --- | ---: | ---: | ---: | ---: | ---: |
| GPT-6 Astra | 52 | 16 | 34 | 2 | 50/52 |
| Claude Fable 5.1 | 51 | 8 | 37 | 6 | 46/51 |

Fable has one unresolved main pair,
`main:novelty-exhaustion-uk:3:1`: its first orientation was the unattributed
route-proof call and was excluded. The other orientation chose Gemma. The
missing orientation is not counted as a tie or a preference.

Among the 51 main pairs complete for both judges, both chose Gemma on 31 and
Qwen on 8. They disagreed, or at least one returned a tie, on 12. The 39 common
wins break down as follows:

| Slice | Qwen | Gemma |
| --- | ---: | ---: |
| Russian | 1 | 13 |
| English | 4 | 9 |
| Ukrainian | 3 | 9 |
| `first` | 0 | 11 |
| `next` | 4 | 20 |
| `reflect` | 4 | 0 |

Three concrete agreed outcomes illustrate what the judges compared:

- `ordinary-ru-first`, sample 3. Qwen: «Что в этом дне осталось незамеченным,
  хотя и было важно?» Gemma: «Что из произошедшего сегодня заслуживает того,
  чтобы об этом вспомнить в молитве?» Both chose Gemma. In the orientation
  where Gemma was A, Astra wrote: «A прямо помогает найти тему молитвы в
  прожитом дне, тогда как B заранее предполагает, что человек упустил что-то
  важное»; Fable wrote: «A естественен и ведёт к молитве; B предполагает, что
  было что-то важное незамеченное».
- `choice-en-next`, sample 2. Qwen: “What would you want her to know if you
  chose to stay?” Gemma: “What would it look like for your mother to be
  supported if you took the position?” Both chose Gemma. With Gemma shown as
  B, Astra wrote: “It makes the concrete tension between taking the job and
  leaving their mother alone actionable without prescribing a choice”; Fable
  wrote: “B takes up the named tension and asks about concrete support if they
  go; A is vague and sidesteps the choice.”
- `grief-ru-reflect`, sample 3. Qwen: «Что бы изменилось, если бы ты начала
  доверять этому спокойствию прямо сейчас?» Gemma: «О чем из этого хочется
  попросить Бога сейчас?» Both chose Qwen. With Qwen shown as B, Astra wrote:
  «Вопрос B связывает бабушкино спокойствие с названной спешкой и предлагает
  задуматься, что изменится сейчас»; Fable wrote: «B держится за слова человека
  (доверие спокойствию) и спрашивает о действии; A общий».

These examples illustrate the measured fixture only; they are not independently
labelled ground truth.

Both judges resolved four of the five controls in favour of one Qwen sample and
returned one tie; both orientations agreed for all five. Because each control
compares different stochastic answers and the control count is small, this does
not by itself validate or invalidate either judge. Read it together with the
position balance, orientation agreement, and written reasons.

## Evidence and limits

- Astra: 114/114 verdict rows, all with runtime model proof `gpt-6-astra`, no
  recorded errors. The run consists of one useful first call plus a continuation
  that wrote the remaining 113 rows.
- Fable: 113/114 accepted verdict rows. Both accepted stream-JSON calls contain
  content-bearing assistant events whose model is exactly
  `claude-fable-5-1`; both exited 0. The CLI-reported cost estimate, including
  the excluded pilot, is `$2.705856`; this is not a confirmed cash charge.
- The canonical pair set, manifests, source mapping, verdicts, and per-judge
  metadata are in this directory. `judge_questions.py` supplies the prompt,
  orientation collapse, and report semantics.
- These are correlated synthetic examples, not a statistical sample of all
  conversations. Maria's blind review remains the product-quality decision.
