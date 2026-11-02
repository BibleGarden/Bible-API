"""Offline, no-LLM replay: English-numbered references in en/ru/uk.

Three ways to turn an expansion reference into retrieval windows:

- current: the request language's window whose canonical ID range overlaps
  the reference (before task 123pfqn54zc);
- vectors (path B): the English window overlapping it, searched by its
  stored vector in the request language's index;
- versification (path A, implemented): the request language's window whose
  stored verses, converted to English numbering, overlap it
  (`scripture_select.reference_windows`).

Two inputs: the recorded references of `after-refs.jsonl` (control set,
anchors matched by canonical ID as in `variants.py`) and `MINI`, references
to places numbered differently in `syn`/`ubh` (target in the indexed
translation's own numbering, matched against the candidate's stored chunk).
Query embeddings by local bge-m3, no Gemini call; output `compare.jsonl`
(database connection notices filtered out). Run from the worktree root in a
disposable container: the `bible-api` environment without
EMBEDDING_PROVIDER/ENDPOINT/API_KEY, plus `-e EMBEDDING_PROVIDER=local
-e EMBEDDING_MODEL_PATH=/models/sentence-transformers/bge-m3-...`, then

    python /code/architect/experiments/search-versification-2026-11-02/compare.py \
        /code/architect/experiments/search-refs-2026-11-02/after-refs.jsonl \
        /code/architect/experiments/search-quality-2026-11-01/queries.json
"""

import json
import sys

import scripture_search as search
import scripture_select as select
from database import create_connection
from deadline import Deadline
from embeddings import build_embedding_client
from retrieval import ScriptureRetriever, make_db_passage_loader, parse_canonical_id

# id, {language: query}, English reference, {alias: [(chapter, first, last)]};
# English queries and `bsb` targets (the reference itself) are in `ENGLISH`.
MINI = [
    ("jonah-fish", {"ru": "Иона во чреве кита", "uk": "Йона в череві риби"},
     (32, 1, 17, 17), {"syn": [(2, 1, 1)], "ubh": [(2, 1, 1)]}),
    ("doxology", {"ru": "слава Богу, могущему утвердить вас", "uk": "слава Богові, що може утвердити вас"},
     (52, 16, 25, 27), {"syn": [(14, 24, 26)], "ubh": [(16, 25, 27)]}),
    ("aaron-plague", {"ru": "Аарон остановил язву между мертвыми и живыми", "uk": "Арон зупинив пошесть між мертвими й живими"},
     (4, 16, 46, 48), {"syn": [(16, 46, 48)], "ubh": [(17, 11, 13)]}),
    ("elijah-sent", {"ru": "пророк Илия придет перед днем Господним", "uk": "пророк Ілля прийде перед днем Господнім"},
     (39, 4, 5, 6), {"syn": [(4, 5, 6)], "ubh": [(3, 23, 24)]}),
    ("nebuchadnezzar", {"ru": "Навуходоносор вернул разум и прославил Бога", "uk": "Навуходоносор повернув розум і прославив Бога"},
     (27, 4, 34, 37), {"syn": [(4, 31, 34)], "ubh": [(4, 31, 34)]}),
    ("return-israel", {"ru": "обратись, Израиль, к Господу", "uk": "повернись, Ізраїлю, до Господа"},
     (28, 14, 1, 2), {"syn": [(14, 2, 3)], "ubh": [(14, 2, 3)]}),
    ("grace-blessing", {"ru": "благодать Господа Иисуса Христа и общение Святого Духа", "uk": "благодать Господа Ісуса Христа і причастя Святого Духа"},
     (54, 13, 14, 14), {"syn": [(13, 13, 13)], "ubh": [(13, 13, 13)]}),
    ("leviathan", {"ru": "левиафан", "uk": "левіятан"},
     (18, 41, 1, 8), {"syn": [(40, 20, 27)], "ubh": [(40, 25, 32)]}),
    ("child-born", {"ru": "младенец родился нам", "uk": "дитя народилося нам"},
     (23, 9, 6, 7), {"syn": [(9, 6, 7)], "ubh": [(9, 5, 6)]}),
    ("wise-boast", {"ru": "да не хвалится мудрый мудростью своею", "uk": "хай не хвалиться мудрий мудрістю своєю"},
     (24, 9, 23, 24), {"syn": [(9, 23, 24)], "ubh": [(9, 22, 23)]}),
    ("solomon-wisdom", {"ru": "мудрость Соломона", "uk": "мудрість Соломона"},
     (11, 4, 29, 34), {"syn": [(4, 29, 34)], "ubh": [(5, 9, 14)]}),
    ("frogs", {"ru": "казнь жабами в Египте", "uk": "кара жабами в Єгипті"},
     (2, 8, 1, 4), {"syn": [(8, 1, 4)], "ubh": [(7, 26, 29)]}),
    ("few-words", {"ru": "пусть слова твои будут немноги перед Богом", "uk": "хай слова твої будуть нечисленні перед Богом"},
     (21, 5, 1, 2), {"syn": [(4, 17, 17), (5, 1, 1)], "ubh": [(4, 17, 17), (5, 1, 1)]}),
]

ENGLISH = {
    "jonah-fish": "Jonah in the belly of the fish",
    "doxology": "glory to him who is able to strengthen you",
    "aaron-plague": "Aaron stopped the plague between the dead and the living",
    "elijah-sent": "Elijah the prophet will come before the day of the Lord",
    "nebuchadnezzar": "Nebuchadnezzar's sanity restored and he praised God",
    "return-israel": "return, O Israel, to the Lord",
    "grace-blessing": "the grace of the Lord Jesus Christ and the fellowship of the Holy Spirit",
    "leviathan": "Leviathan",
    "child-born": "unto us a child is born",
    "wise-boast": "let not the wise man boast in his wisdom",
    "solomon-wisdom": "the wisdom of Solomon",
    "frogs": "the plague of frogs in Egypt",
    "few-words": "let your words be few before God",
}
MINI = [
    (case_id, {"en": ENGLISH[case_id], **texts}, reference,
     {"bsb": [reference[1:]], **targets})
    for case_id, texts, reference, targets in MINI
]
ALIASES = {"en": "bsb", "ru": "syn", "uk": "ubh"}


def rank_of(candidates, anchor):
    book, chapter, first, last = anchor
    for position, canonical_id in enumerate(candidates, 1):
        _v, b, c, start, end = parse_canonical_id(canonical_id)
        if (b, c) == (book, chapter) and end >= first and start <= last:
            return position
    return None


def target_rank(candidates, book, targets):
    """Position of the first candidate whose stored chunk holds a target."""
    for position, candidate in enumerate(candidates, 1):
        passage = candidate.passages[0]
        if passage.book_number == book and any(
            passage.chapter_number == c
            and passage.verse_number_end >= first
            and passage.verse_number_start <= last
            for c, first, last in targets
        ):
            return position
    return None


def main(records, queries):
    cases = {case["id"]: case for case in json.load(open(queries))}
    resources = select.get_resources(allow_stale=False)
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    load = make_db_passage_loader(cursor)
    with build_embedding_client(timeout=60, max_retries=1) as embedder:
        def run(language, query, references):
            allowed = select.coverage_filter(
                resources, language, resources.primary[language]
            )
            retriever = ScriptureRetriever(
                index=resources.index, embedder=embedder, rewriter=None,
                load_passages=load, lexical_indexes=resources.lexical,
                blacklist=[], safe_pool=[], allowed_canonical_ids=allowed,
            )
            own_rows = retriever._vector_rows

            def retrieve(windows, rows_language):
                retriever._vector_rows = lambda _language: own_rows(rows_language)
                return retriever.search(language, query, windows, Deadline(120))

            english = search_windows(
                resources.reference_windows["en"], None, references
            )
            return {
                "current": retrieve(search_windows(
                    resources.windows[language], allowed, references
                ), language),
                "vectors": retrieve(english, "en"),
                "versification": retrieve(search_windows(
                    resources.reference_windows[language], allowed, references
                ), language),
            }

        for line in open(records):
            record = json.loads(line)
            case = cases[record["id"]]
            if record["status"] != "ok" or not case["anchors"]:
                continue
            answer = next(
                c["answer"] for c in record["calls"] if c["stage"] == "scripture_rewrite"
            )
            references = [
                (search.BOOK_NUMBERS[p["book"]], p["chapter"], p["verse_start"], p["verse_end"])
                for p in json.loads(answer)["passages"]
            ]
            results = run(case["language"], case["query"], references)
            print(json.dumps({
                "set": "control", "id": record["id"], "run": record["run"],
                **{
                    name: [
                        rank_of([c.canonical_id for c in candidates], anchor)
                        for anchor in case["anchors"]
                    ]
                    for name, candidates in results.items()
                },
            }), flush=True)

        for case_id, texts, reference, targets in MINI:
            for language, query in texts.items():
                results = run(language, query, [reference])
                alias = ALIASES[language]
                print(json.dumps({
                    "set": "mini", "id": case_id, "language": language,
                    **{
                        name: target_rank(candidates, reference[0], targets[alias])
                        for name, candidates in results.items()
                    },
                    "top": {
                        name: [
                            f"{c.passages[0].chapter_number}:"
                            f"{c.passages[0].verse_number_start}-"
                            f"{c.passages[0].verse_number_end}"
                            for c in candidates[:3]
                        ]
                        for name, candidates in results.items()
                    },
                }, ensure_ascii=False), flush=True)


search_windows = search._reference_windows

if __name__ == "__main__":
    main(*sys.argv[1:])
