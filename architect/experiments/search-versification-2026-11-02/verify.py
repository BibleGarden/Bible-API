"""Verse-by-verse check of the versification files against the stored texts.

For every non-Psalm verse of `syn` and `ubh`, `to_reference_numbering` gives
its English verses; the check compares the verse's local bge-m3 embedding
with that of those `bsb` verses (cosine to their normalised mean) and with
every `bsb` verse of the same book in the chapter before, the same and the
next. A verse is flagged when its mapped similarity is low (`LOW`) and
another `bsb` verse is clearly closer (`MARGIN`); flags at most two verses
apart in one chapter form a run. A shifted mapping shows as a long run;
single flags are mostly lists, refrains and paraphrases.

Run from `app/` in a disposable container with the `bible-api` environment
(database) and the model volume:

    python verify.py embed /models/sentence-transformers/bge-m3-... <dir>
    python verify.py check <dir> [base]

`embed` writes `keys.json` and `vectors.npy` (every non-Psalm verse of syn,
ubh and bsb; 79 minutes on 8 CPU cores). `check ... base` checks `syn`
with rso.vrs alone, without syn.vrs.
"""

import json
import statistics
import sys
from collections import defaultdict

import numpy as np

import versification
from versification import to_reference_numbering

LOW = 0.6
MARGIN = 0.1


def embed(model_path, directory):
    from sentence_transformers import SentenceTransformer

    from database import create_connection

    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute(
        "SELECT t.alias, v.book_number, v.chapter_number, v.verse_number, v.text "
        "FROM translation_verses v JOIN translations t ON t.code = v.translation "
        "WHERE t.alias IN ('syn', 'ubh', 'bsb') AND v.book_number != 19 "
        "ORDER BY t.alias, v.book_number, v.chapter_number, v.verse_number"
    )
    rows = cursor.fetchall()
    json.dump([row[:4] for row in rows], open(f"{directory}/keys.json", "w"))
    model = SentenceTransformer(model_path, device="cpu")
    model.max_seq_length = 160
    texts = [row[4] for row in rows]
    order = np.argsort([len(text) for text in texts])  # less padding
    vectors = np.zeros((len(texts), 1024), np.float16)
    vectors[order] = model.encode(
        [texts[i] for i in order], batch_size=64, normalize_embeddings=True
    )
    np.save(f"{directory}/vectors.npy", vectors)


def check(directory, base=None):
    if base:
        versification.TRANSLATION_VERSIFICATIONS["syn"] = ("rso",)
    keys = [tuple(k) for k in json.load(open(f"{directory}/keys.json"))]
    vectors = np.load(f"{directory}/vectors.npy").astype(np.float32)
    row = {key: i for i, key in enumerate(keys)}
    bsb = defaultdict(list)  # (book, chapter) -> [(verse, row)]
    for (alias, book, chapter, verse), i in row.items():
        if alias == "bsb":
            bsb[(book, chapter)].append((verse, i))
    for alias in (("syn",) if base else ("syn", "ubh")):
        similarities, flags, unmapped = [], [], 0
        for (a, book, chapter, verse), i in row.items():
            if a != alias:
                continue
            targets = [
                row[("bsb", b, c, v)]
                for b, c, first, last in to_reference_numbering(
                    alias, book, chapter, verse, verse
                )
                for v in range(first, last + 1)
                if ("bsb", b, c, v) in row
            ]
            if not targets:
                unmapped += 1
                continue
            mean = vectors[targets].mean(axis=0)
            mapped = float(vectors[i] @ mean / np.linalg.norm(mean))
            similarities.append(mapped)
            near = [
                (c, v, r)
                for c in (chapter - 1, chapter, chapter + 1)
                for v, r in bsb.get((book, c), [])
                if r not in targets
            ]
            if mapped >= LOW or not near:
                continue
            scores = vectors[[r for _c, _v, r in near]] @ vectors[i]
            best = int(np.argmax(scores))
            if scores[best] > mapped + MARGIN:
                c, v, _r = near[best]
                flags.append(((book, chapter, verse), round(mapped, 2),
                              (book, c, v), round(float(scores[best]), 2)))
        runs = []
        for flag in flags:
            (book, chapter, verse) = flag[0]
            if runs and runs[-1][-1][0][:2] == (book, chapter) and \
                    verse - runs[-1][-1][0][2] <= 2:
                runs[-1].append(flag)
            else:
                runs.append([flag])
        print(json.dumps({
            "alias": alias,
            "pairs": len(similarities),
            "unmapped": unmapped,
            "median": round(statistics.median(similarities), 3),
            "below_0.5": sum(s < 0.5 for s in similarities),
            "flagged": len(flags),
            "flags": flags,
            "runs_of_3_or_more": [
                [run[0][0], run[-1][0][2], len(run), run[0][2]]
                for run in runs if len(run) >= 3
            ],
        }))


if __name__ == "__main__":
    {"embed": embed, "check": check}[sys.argv[1]](*sys.argv[2:])
