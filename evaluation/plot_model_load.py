#!/usr/bin/env python3
"""Render the Qwen/Gemma load comparison for ClickUp 86cbh0p8t.

Matplotlib is an evaluation-only dependency and is not added to Bible-API.
Reproduce the checked-in PNG, SVG and PDF in an ephemeral environment::

    python3 -m venv /tmp/86cbh0p8t-plot
    /tmp/86cbh0p8t-plot/bin/pip install matplotlib==3.10.6
    /tmp/86cbh0p8t-plot/bin/python evaluation/plot_model_load.py

The committed artifacts were rendered with Matplotlib 3.10.6 using this
repository's Python 3.12 interpreter.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

CONCURRENCY = (1, 2, 4, 8, 16)
STAGES = (
    ("question", "Наводящий вопрос"),
    ("rewrite", "Rewrite для поиска Писания"),
    ("rerank", "Rerank отрывков"),
)
MODELS = {
    "Qwen3-30B-A3B FP8": {
        "color": "#2878B5",
        "files": (
            "qwen_production_load_2026-09-12.json",
            "qwen_load_c8_2026-09-12.json",
            "qwen_load_c16_2026-09-12.json",
        ),
    },
    "Gemma 4 31B FP8": {
        "color": "#D65F5F",
        "files": (
            "gemma_load_c1_2026-09-12.json",
            "gemma_load_c2_c16_2026-09-12.json",
        ),
    },
}


def load_model(data_dir: Path, files: tuple[str, ...]) -> dict[int, dict]:
    levels: dict[int, dict] = {}
    for filename in files:
        payload = json.loads((data_dir / filename).read_text())
        for level in payload["levels"]:
            concurrency = int(level["concurrency"])
            if concurrency in levels:
                raise ValueError(f"duplicate concurrency {concurrency} in {filename}")
            summary = level["summary"]
            if summary["errors"] or summary["invalid_responses"]:
                raise ValueError(f"failed requests at concurrency {concurrency}")
            for stage, _label in STAGES:
                if summary["by_stage"][stage]["requests"] != 20:
                    raise ValueError(
                        f"expected 20 {stage} requests at concurrency {concurrency}"
                    )
            levels[concurrency] = summary
    if tuple(sorted(levels)) != CONCURRENCY:
        raise ValueError(
            f"expected concurrency {CONCURRENCY}, got {tuple(sorted(levels))}"
        )
    return levels


def render(data_dir: Path, output_prefix: Path) -> None:
    series = {
        name: load_model(data_dir, config["files"]) for name, config in MODELS.items()
    }
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.titleweight": "bold",
            "axes.spines.top": False,
            "axes.spines.right": False,
        }
    )
    figure, axes = plt.subplots(2, 2, figsize=(12.5, 8.3))
    figure.suptitle(
        "Bible-API: сравнение скорости Qwen и Gemma",
        fontsize=17,
        fontweight="bold",
    )

    for axis, (stage, title) in zip(axes.flat[:3], STAGES, strict=True):
        for name, config in MODELS.items():
            levels = series[name]
            color = config["color"]
            p50 = [
                levels[value]["by_stage"][stage]["latency_p50_seconds"]
                for value in CONCURRENCY
            ]
            p95 = [
                levels[value]["by_stage"][stage]["latency_p95_seconds"]
                for value in CONCURRENCY
            ]
            axis.plot(CONCURRENCY, p50, color=color, marker="o", linewidth=2.3)
            axis.plot(
                CONCURRENCY,
                p95,
                color=color,
                marker="o",
                linewidth=1.8,
                linestyle="--",
                alpha=0.9,
            )
        axis.set_title(title)
        axis.set_xlabel("Одновременные запросы")
        axis.set_ylabel("Полная задержка, секунд")
        axis.set_xticks(CONCURRENCY)
        axis.grid(axis="y", alpha=0.25)

    throughput = axes.flat[3]
    for name, config in MODELS.items():
        levels = series[name]
        throughput.plot(
            CONCURRENCY,
            [levels[value]["throughput_requests_per_second"] for value in CONCURRENCY],
            color=config["color"],
            marker="o",
            linewidth=2.3,
        )
    throughput.set_title("Общая пропускная способность")
    throughput.set_xlabel("Одновременные запросы")
    throughput.set_ylabel("Завершённых запросов в секунду")
    throughput.set_xticks(CONCURRENCY)
    throughput.grid(axis="y", alpha=0.25)

    legend = [
        Line2D([0], [0], color=config["color"], lw=2.5, label=name)
        for name, config in MODELS.items()
    ]
    legend.extend(
        [
            Line2D([0], [0], color="#555555", lw=2.2, label="p50"),
            Line2D([0], [0], color="#555555", lw=1.8, linestyle="--", label="p95"),
        ]
    )
    figure.legend(
        handles=legend,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.94),
        ncol=4,
        frameon=False,
    )
    figure.text(
        0.5,
        0.012,
        "Qwen: RTX 4090 48 ГБ (по данным владельца); Gemma: L40S 48 ГБ. "
        "Разные стенды и сетевые пути; прогретый кэш; повторяющийся короткий набор; "
        "n=20 на стадию и уровень. p95 — исследовательская оценка.",
        ha="center",
        fontsize=9,
        color="#555555",
    )
    figure.tight_layout(rect=(0, 0.055, 1, 0.89))
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    for extension in ("png", "svg", "pdf"):
        figure.savefig(
            output_prefix.with_suffix(f".{extension}"),
            dpi=180 if extension == "png" else None,
            bbox_inches="tight",
        )
    plt.close(figure)


def main() -> None:
    parser = argparse.ArgumentParser()
    default_dir = Path(__file__).with_name("bench_data") / "runpod_86cbh0p8t"
    parser.add_argument("--data-dir", type=Path, default=default_dir)
    parser.add_argument(
        "--output-prefix", type=Path, default=default_dir / "model_load_comparison"
    )
    args = parser.parse_args()
    render(args.data_dir, args.output_prefix)


if __name__ == "__main__":
    main()
