#!/usr/bin/env python3
"""
compare_toc.py — Compare parser output against a reference ToC and flag issues.

Structural flags (no reference needed):
  NO_AUTHOR   — author field empty
  SWALLOWED   — text length > Q3 + 3×IQR, or text contains embedded headings (##)
  SHORT_TEXT  — text < 80 chars
  PAGE_GAP    — source_page gap > 15 between consecutive poems

Reference flags (requires --toc-file):
  MISSING     — reference entry has no matching extracted poem
  EXTRA       — extracted poem has no matching reference entry
  PAGE_MISMATCH — title matched but page numbers differ by > 3

Usage:
    python compare_toc.py --pdf data/Golden_Treasury.pdf
    python compare_toc.py --pdf data/Golden_Treasury.pdf --toc-file data/Golden_Treasury_toc.csv

Reference CSV format (--toc-file):
    title,page
    A Madrigal,12
    Spring,16
    (optional third column: author)
"""

import argparse
import csv
import json
import re
import sys
import statistics
from difflib import SequenceMatcher
from pathlib import Path


def normalize_title(title: str) -> str:
    t = title.lower()
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def load_poems(poems_path: Path) -> list[dict]:
    with open(poems_path, encoding="utf-8") as f:
        return json.load(f)


def load_toc_file(toc_path: Path) -> list[dict]:
    entries = []
    with open(toc_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            title = row.get("title", "").strip()
            if not title:
                continue
            try:
                page = int(row.get("page", 0))
            except (ValueError, TypeError):
                page = 0
            entries.append({
                "title": title,
                "page": page,
                "author": row.get("author", "").strip(),
            })
    return entries


def compute_structural_flags(poems: list[dict]) -> list[list[str]]:
    lengths = [len(p.get("text", "")) for p in poems]

    if len(lengths) >= 4:
        q1, _, q3 = statistics.quantiles(lengths, n=4)
        swallowed_threshold = q3 + 3 * (q3 - q1)
    else:
        swallowed_threshold = float("inf")

    all_flags: list[list[str]] = []
    prev_page: int | None = None

    for poem, text_len in zip(poems, lengths):
        flags: list[str] = []
        text = poem.get("text", "")
        author = poem.get("author", "").strip()
        page = poem.get("source_page", 0)

        if not author:
            flags.append("NO_AUTHOR")
        if text_len < 80:
            flags.append("SHORT_TEXT")
        if text_len > swallowed_threshold or re.search(r"^#{2,}", text, re.MULTILINE):
            flags.append("SWALLOWED")
        if prev_page is not None and (page - prev_page) > 15:
            flags.append("PAGE_GAP")

        prev_page = page
        all_flags.append(flags)

    return all_flags


def _word_overlap(a: str, b: str) -> float:
    """Fraction of the shorter string's words found in the longer string's word set."""
    wa = set(a.split())
    wb = set(b.split())
    if not wa or not wb:
        return 0.0
    shorter = wa if len(wa) <= len(wb) else wb
    longer = wb if len(wa) <= len(wb) else wa
    return len(shorter & longer) / len(shorter)


def align_with_reference(
    poems: list[dict],
    ref_entries: list[dict],
) -> tuple[list[int | None], list[int | None]]:
    """
    Align poems to reference entries by title using three passes:

      1. Exact normalized match
      2. Character-level fuzzy (SequenceMatcher ≥ 0.80)
      3. Word-token overlap (≥ 0.65) + page proximity (|ref_page - poem_page| ≤ 5)
         — handles OCR noise that corrupts individual characters but leaves most
           words intact (e.g. "Exho's" vs "Echo's", "Hohday" vs "Holiday")

    Returns:
        ref_match[i]  — index of matching poem for reference entry i, or None
        poem_match[j] — index of matching reference entry for poem j, or None
    """
    norm_poems = [normalize_title(p.get("title", "")) for p in poems]
    norm_refs = [normalize_title(r["title"]) for r in ref_entries]
    poem_pages = [p.get("source_page", 0) for p in poems]
    ref_pages = [r.get("page", 0) for r in ref_entries]

    matched_poem_idxs: set[int] = set()
    matched_ref_idxs: set[int] = set()
    ref_match: list[int | None] = [None] * len(ref_entries)
    poem_match: list[int | None] = [None] * len(poems)

    def _commit(i: int, j: int) -> None:
        ref_match[i] = j
        poem_match[j] = i
        matched_poem_idxs.add(j)
        matched_ref_idxs.add(i)

    # Pass 1: exact normalized matches
    for i, nref in enumerate(norm_refs):
        for j, npoe in enumerate(norm_poems):
            if j in matched_poem_idxs:
                continue
            if nref == npoe:
                _commit(i, j)
                break

    # Pass 2: character-level fuzzy (SequenceMatcher ≥ 0.80)
    for i, nref in enumerate(norm_refs):
        if i in matched_ref_idxs:
            continue
        best_score = 0.0
        best_j: int | None = None
        for j, npoe in enumerate(norm_poems):
            if j in matched_poem_idxs:
                continue
            score = SequenceMatcher(None, nref, npoe).ratio()
            if score > best_score:
                best_score = score
                best_j = j
        if best_j is not None and best_score >= 0.80:
            _commit(i, best_j)

    # Pass 3: word-token overlap + page proximity for OCR-noisy titles.
    # Requires ≥ 2 words in the shorter title to avoid spurious single-word matches.
    for i, nref in enumerate(norm_refs):
        if i in matched_ref_idxs:
            continue
        ref_words = nref.split()
        if len(ref_words) < 2:
            continue
        best_score = 0.0
        best_j: int | None = None
        for j, npoe in enumerate(norm_poems):
            if j in matched_poem_idxs:
                continue
            if abs(ref_pages[i] - poem_pages[j]) > 5:
                continue
            overlap = _word_overlap(nref, npoe)
            if overlap > best_score:
                best_score = overlap
                best_j = j
        if best_j is not None and best_score >= 0.65:
            _commit(i, best_j)

    return ref_match, poem_match


def compute_fidelity(
    poems: list[dict],
    all_flags: list[list[str]],
    ref_entries: list[dict] | None,
    ref_match: list[int | None] | None,
) -> dict:
    """
    Compute fidelity metrics from structural and reference flags.

    Reference metrics (recall, precision, F1) require --toc-file.
    Structural metrics (boundary, completeness, author) are always computed.

    Overall fidelity:
      With reference:    0.5 * F1 + 0.3 * boundary + 0.2 * author_coverage
      Without reference: 0.6 * boundary + 0.4 * author_coverage
    """
    n = len(poems)
    counts: dict[str, int] = {}
    for flags in all_flags:
        for f in flags:
            counts[f] = counts.get(f, 0) + 1

    swallowed     = counts.get("SWALLOWED", 0)
    short_text    = counts.get("SHORT_TEXT", 0)
    no_author     = counts.get("NO_AUTHOR", 0)
    page_gap      = counts.get("PAGE_GAP", 0)
    extra         = counts.get("EXTRA", 0)
    page_mismatch = counts.get("PAGE_MISMATCH", 0)

    # Structural scores (poem-level)
    boundary_score    = 1 - swallowed / n if n else 0.0
    completeness_score = 1 - short_text / n if n else 0.0
    author_score       = 1 - no_author / n if n else 0.0

    scores: dict = {
        "total_poems": n,
        "swallowed": swallowed,
        "short_text": short_text,
        "no_author": no_author,
        "page_gap": page_gap,
        "boundary_score": boundary_score,
        "completeness_score": completeness_score,
        "author_score": author_score,
    }

    if ref_entries is not None and ref_match is not None:
        ref_n = len(ref_entries)
        missing_count = sum(1 for m in ref_match if m is None)
        matched = ref_n - missing_count
        recall    = matched / ref_n if ref_n else 0.0
        precision = matched / n if n else 0.0
        f1 = (2 * recall * precision / (recall + precision)
              if (recall + precision) > 0 else 0.0)
        fidelity = 0.5 * f1 + 0.3 * boundary_score + 0.2 * author_score
        scores.update({
            "total_ref": ref_n,
            "matched": matched,
            "missing": missing_count,
            "extra": extra,
            "page_mismatch": page_mismatch,
            "recall": recall,
            "precision": precision,
            "f1": f1,
        })
    else:
        fidelity = 0.6 * boundary_score + 0.4 * author_score

    scores["fidelity"] = fidelity
    return scores


def print_fidelity(scores: dict) -> None:
    print("\n  ── Fidelity ─────────────────────────────")
    if "recall" in scores:
        matched = scores["matched"]
        ref_n   = scores["total_ref"]
        n       = scores["total_poems"]
        print(f"  Recall (coverage)   {scores['recall']:>6.1%}   ({matched}/{ref_n} reference entries found)")
        print(f"  Precision           {scores['precision']:>6.1%}   ({matched}/{n} extracted poems matched)")
        print(f"  F1                  {scores['f1']:>6.1%}")
    print(f"  Boundary quality    {scores['boundary_score']:>6.1%}   ({scores['swallowed']} SWALLOWED)")
    print(f"  Text completeness   {scores['completeness_score']:>6.1%}   ({scores['short_text']} SHORT_TEXT)")
    print(f"  Author coverage     {scores['author_score']:>6.1%}   ({scores['no_author']} NO_AUTHOR)")
    print(f"  {'─' * 38}")
    print(f"  Fidelity score      {scores['fidelity']:>6.1%}")


def _sort_key(row: dict) -> int:
    for field in ("ref_page", "parser_page"):
        v = row.get(field, "")
        if v != "":
            try:
                return int(v)
            except (ValueError, TypeError):
                pass
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare parser output against a reference ToC and flag issues."
    )
    parser.add_argument("--pdf", required=True, help="Path to the anthology PDF")
    parser.add_argument("--toc-file", help="Reference ToC CSV (title, page [, author])")
    parser.add_argument(
        "--output-dir", default="pipeline_output/marker_poems",
        help="Where to find _poems.json and write review CSV (default: pipeline_output/marker_poems)"
    )
    args = parser.parse_args()

    pdf_path = Path(args.pdf)
    stem = pdf_path.stem
    output_dir = Path(args.output_dir)
    poems_path = output_dir / f"{stem}_poems.json"

    if not poems_path.exists():
        print(f"[error] Not found: {poems_path}", file=sys.stderr)
        sys.exit(1)

    poems = load_poems(poems_path)
    poems.sort(key=lambda p: p.get("source_page", 0))

    structural_flags = compute_structural_flags(poems)
    all_flags = [list(sf) for sf in structural_flags]

    ref_entries: list[dict] | None = None
    ref_match: list[int | None] | None = None
    poem_match: list[int | None] | None = None

    if args.toc_file:
        toc_path = Path(args.toc_file)
        if not toc_path.exists():
            print(f"[error] ToC file not found: {toc_path}", file=sys.stderr)
            sys.exit(1)
        ref_entries = load_toc_file(toc_path)
        ref_match, poem_match = align_with_reference(poems, ref_entries)

        for j in range(len(poems)):
            ri = poem_match[j]
            if ri is None:
                all_flags[j].append("EXTRA")
            else:
                ref_page = ref_entries[ri]["page"]
                parser_page = poems[j].get("source_page", 0)
                if abs(parser_page - ref_page) > 3:
                    all_flags[j].append("PAGE_MISMATCH")

    # ── Terminal summary ──────────────────────────────────────────────────────

    flag_counts: dict[str, int] = {}
    for flags in all_flags:
        for f in flags:
            flag_counts[f] = flag_counts.get(f, 0) + 1

    flagged = [(poems[j], all_flags[j]) for j in range(len(poems)) if all_flags[j]]

    print(f"\n[compare] {stem}")
    print(f"  Poems extracted: {len(poems)}")
    if ref_entries is not None:
        missing_count = sum(1 for m in ref_match if m is None)
        print(f"  Reference entries: {len(ref_entries)}")
        print(f"  Missing (in ref, not extracted): {missing_count}")
    print(f"  Flagged: {len(flagged)}")
    for code, count in sorted(flag_counts.items()):
        print(f"    {code:<16} {count}")

    if flagged:
        col_w = 42
        print(f"\n  {'Page':>5}  {'Title':<{col_w}}  {'Author':<24}  Flags")
        print("  " + "-" * (col_w + 42))
        for poem, flags in flagged:
            title = (poem.get("title", "") or "")[:col_w]
            author = (poem.get("author", "") or "")[:24]
            page = poem.get("source_page", 0)
            print(f"  {page:>5}  {title:<{col_w}}  {author:<24}  {', '.join(flags)}")

    if ref_entries is not None and ref_match is not None:
        missing_refs = [ref_entries[i] for i, m in enumerate(ref_match) if m is None]
        if missing_refs:
            print(f"\n  MISSING ({len(missing_refs)} reference entries not extracted):")
            for ref in missing_refs:
                print(f"    p{ref['page']:>4}  {ref['title']}")

    scores = compute_fidelity(poems, all_flags, ref_entries, ref_match)
    print_fidelity(scores)

    # ── CSV output ────────────────────────────────────────────────────────────

    rows: list[dict] = []

    for j, poem in enumerate(poems):
        ri = poem_match[j] if poem_match is not None else None
        ref = ref_entries[ri] if (ref_entries is not None and ri is not None) else {}
        text = poem.get("text", "") or ""
        rows.append({
            "idx": j,
            "ref_title": ref.get("title", ""),
            "ref_page": ref.get("page", ""),
            "parser_title": poem.get("title", ""),
            "parser_page": poem.get("source_page", ""),
            "author": poem.get("author", ""),
            "author_source": poem.get("author_source", ""),
            "text_length": len(text),
            "flags": "|".join(all_flags[j]),
            "text_preview": text.replace("\n", " ")[:120],
            "manual_author": "",
        })

    if ref_entries is not None and ref_match is not None:
        for i, ref in enumerate(ref_entries):
            if ref_match[i] is None:
                rows.append({
                    "idx": "",
                    "ref_title": ref["title"],
                    "ref_page": ref["page"],
                    "parser_title": "",
                    "parser_page": "",
                    "author": ref.get("author", ""),
                    "author_source": "",
                    "text_length": "",
                    "flags": "MISSING",
                    "text_preview": "",
                    "manual_author": "",
                })

    rows.sort(key=_sort_key)

    fields = [
        "idx", "ref_title", "ref_page", "parser_title", "parser_page",
        "author", "author_source", "text_length", "flags", "text_preview", "manual_author",
    ]
    csv_path = output_dir / f"{stem}_toc_review.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n[compare] Review CSV → {csv_path}")


if __name__ == "__main__":
    main()
