#!/usr/bin/env python3
"""
parse_marker.py — Poem anthology parser using Marker's JSON output + ToC mapping.

Uses marker's PdfConverter to extract structured JSON blocks, then maps the
table_of_contents from metadata to individual poems using polygon-based
spatial boundary detection.

Key behaviour:
  - ToC entries are classified as 'poet', 'collection', or 'poem' by heuristic.
  - Empty ToC entries are skipped.
  - Speaker labels within dramatic poems (e.g. "Second Nymph.") are merged back
    into the preceding poem unless --no_merge_speakers is passed.
  - PageHeader / PageFooter / Footnote blocks are stripped from poem text.

Usage:
    python parse_marker.py --input data/anthology.pdf
    python parse_marker.py --input data/anthology.pdf --output_format json_combined
    python parse_marker.py --input data/anthology.pdf --output_format markdown
    python parse_marker.py --input data/ --output_format csv
"""

import argparse
import csv
import json
import re
import statistics
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

from marker.schema import BlockTypes
from marker.schema.text.line import Line
from marker.schema.blocks import Text as TextBlock
from marker.schema.registry import register_block_class
from marker.processors import BaseProcessor


class PoetryLine(Line):
    """Appends <br> after each line so _block_html_text can convert it to \\n."""
    def assemble_html(self, document, child_blocks, parent_structure, block_config):
        template = super().assemble_html(
            document, child_blocks, parent_structure, block_config
        )
        return template + "<br>"


class StanzaBreakProcessor(BaseProcessor):
    """
    Splits Text blocks at large vertical gaps between their child Line blocks,
    so that each stanza becomes its own Text block.

    After splitting, the existing join logic produces blank lines between stanzas:
    each Text block's text ends with \\n (from PoetryLine's <br>), so
    '\\n'.join(poem_text_parts) naturally yields \\n\\n at stanza boundaries.
    """
    block_types = (BlockTypes.Text,)
    stanza_gap_multiplier = 1.75  # gap / median_gap threshold

    def __call__(self, document):
        for page in document.pages:
            original_structure = list(page.structure)
            insertions = []  # list of (orig_block_id, [new_block_ids])

            for block_id in original_structure:
                block = page.get_block(block_id)
                if block.block_type != BlockTypes.Text:
                    continue
                if block.structure is None or len(block.structure) < 2:
                    continue

                lines = block.contained_blocks(document, (BlockTypes.Line,))
                if len(lines) < 2:
                    continue

                gaps = [lines[i].polygon.y_start - lines[i-1].polygon.y_end
                        for i in range(1, len(lines))]
                median_gap = statistics.median(gaps)
                if median_gap <= 0:
                    continue
                threshold = self.stanza_gap_multiplier * median_gap

                # Find indices where a new stanza begins (gap > threshold).
                # Require each resulting group to have >= 2 lines to avoid
                # false splits caused by indented short lines (e.g. song lyrics
                # with alternating long/short lines where the short line's
                # bounding box skews the gap ratio).
                raw_splits = [
                    i for i in range(1, len(lines))
                    if gaps[i - 1] > threshold
                ]
                split_at = [i for i in raw_splits
                            if i >= 2 and i <= len(lines) - 2]
                if not split_at:
                    continue

                # Partition lines into per-stanza groups
                boundaries = [0] + split_at + [len(lines)]
                groups = [lines[boundaries[i]:boundaries[i+1]]
                          for i in range(len(boundaries) - 1)]

                # First group stays in the original block
                first = groups[0]
                block.structure = [l.id for l in first]
                block.polygon = first[0].polygon.merge(
                    [l.polygon for l in first[1:]]
                )

                # Create new Text blocks for subsequent stanzas
                new_ids = []
                for group in groups[1:]:
                    poly = group[0].polygon.merge([l.polygon for l in group[1:]])
                    new_block = TextBlock(
                        polygon=poly,
                        page_id=page.page_id,
                        structure=[l.id for l in group],
                    )
                    page.add_full_block(new_block)
                    new_ids.append(new_block.id)

                insertions.append((block_id, new_ids))

            # Insert new block IDs into page.structure after their parent
            for orig_id, new_ids in insertions:
                idx = page.structure.index(orig_id)
                for j, new_id in enumerate(new_ids):
                    page.structure.insert(idx + 1 + j, new_id)


# ─────────────────────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Poem:
    title: str = ""
    author: str = ""
    collection: str = ""        # e.g. "[From The Forest.]" sub-header
    source_anthology: str = ""
    source_page: int = 0
    text: str = ""
    notes: str = ""
    needs_review: bool = False
    review_reason: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# ToC classification
# ─────────────────────────────────────────────────────────────────────────────

# Genre/form keywords that look all-caps but are poem-type headings, not poet names
_GENRE_WORDS = {
    "SONG", "SONGS", "ODE", "ODES", "HYMN", "HYMNS", "SONNET", "SONNETS",
    "EPIGRAM", "EPIGRAMS", "ELEGY", "ELEGIES", "EPISTLE", "EPISTLES",
    "SATIRE", "SATIRES", "BALLAD", "BALLADS", "PROLOGUE", "EPILOGUE",
    "CHORUS", "DIRGE", "EPITAPH", "EPITAPHS", "MADRIGAL", "SEXTAIN",
}


def classify_toc_entry(title: str) -> Optional[str]:
    """
    Return 'poet', 'collection', or 'poem' for a ToC title, or None to skip.

    Rules (applied in order):
      1. Empty / whitespace-only  → None (skip)
      2. Starts with '['          → 'collection'  (e.g. "[From The Forest.]")
      3. All-caps, ≤6 words, not a genre word  → 'poet'
      4. Anything else            → 'poem'
    """
    t = title.strip()
    if not t:
        return None

    # Collection sub-headers: bracketed editorial notes
    if t.startswith("["):
        return "collection"

    # Poet headers: predominantly uppercase short phrases
    # Allow OCR noise like lowercase letters mixed in (e.g. "BEN yONSON")
    words = t.split()
    if len(words) <= 6:
        # Strip OCR junk chars for ratio calculation
        alpha = [c for c in t if c.isalpha()]
        if alpha:
            upper_ratio = sum(1 for c in alpha if c.isupper()) / len(alpha)
            if upper_ratio >= 0.65:
                # Exclude genre/form words that happen to be all-caps
                cleaned = re.sub(r"[^A-Za-z\s]", "", t).strip().upper()
                if cleaned not in _GENRE_WORDS and cleaned.rstrip("S") not in _GENRE_WORDS:
                    # Exclude common poem-starter words that can appear in short all-caps lines
                    poem_starters = {"AND", "THE", "BUT", "OR", "IF", "TO", "IN",
                                     "AS", "THAT", "WITH", "FOR", "NOT", "ALL",
                                     "MY", "HER", "HIS", "ON", "WHERE", "WHEN"}
                    first_word = words[0].upper().strip(".,;:")
                    if first_word not in poem_starters:
                        return "poet"

    return "poem"


# ─────────────────────────────────────────────────────────────────────────────
# Block helpers
# ─────────────────────────────────────────────────────────────────────────────

_SKIP_BLOCK_TYPES = {"PageHeader", "PageFooter", "Footnote"}

# Block types whose content is structural, not poem body
_STRUCTURAL_BLOCK_TYPES = {"SectionHeader"}


def _page_id_from_block(block) -> int:
    """
    Extract integer page ID from a block's id string.
    Marker IDs look like '/page/28/Page/0' or '/page/28/SectionHeader/2'.
    """
    parts = block.id.split("/")
    try:
        return int(parts[2])
    except (IndexError, ValueError):
        return -1


def _block_top_y(block) -> float:
    """Top y-coordinate of a block's polygon (first vertex)."""
    return block.polygon[0][1]


def _block_html_text(block) -> str:
    """Strip HTML tags and return plain text, preserving <br> as newlines.

    For Text blocks, also re-splits lines that Marker merged at extraction time.
    Two passes (high → low confidence) catch both punctuated and unpunctuated
    line boundaries common in early modern poetry.
    """
    html = re.sub(r"<br\s*/?>", "\n", block.html, flags=re.IGNORECASE)
    # Strip only horizontal whitespace — preserve trailing \n so that
    # "\n".join(poem_text_parts) produces \n\n (blank line) between blocks.
    text = re.sub(r"<[^>]+>", "", html).strip(" \t")
    if block.block_type == "Text":
        # Split where sentence-ending punctuation precedes a capital.
        # Catches merged lines like "found. All" or "compriseth ! Do".
        # Deliberately excludes lowercase-before-capital to avoid splitting proper nouns.
        text = re.sub(r"([.!?;])\s+([A-Z])", r"\1\n\2", text)
    return text


# ─────────────────────────────────────────────────────────────────────────────
# Main mapping function
# ─────────────────────────────────────────────────────────────────────────────

def map_toc_to_poems(
    rendered,
    anthology_name: str,
    merge_speakers: bool = True,
) -> list[Poem]:
    """
    Map marker's JSONOutput to a list of Poems using the table_of_contents.

    Parameters
    ----------
    rendered : JSONOutput
        The object returned by PdfConverter (has .children and .metadata).
    anthology_name : str
        Used to populate Poem.source_anthology.
    merge_speakers : bool
        If True, ToC entries that look like speaker labels within a dramatic
        poem (same page, within ~150px of the previous poem entry) are merged
        back into the preceding poem rather than becoming separate entries.
    """
    toc = rendered.metadata.get("table_of_contents", [])
    if not toc:
        print("[warn] table_of_contents is empty — no poems extracted.", file=sys.stderr)
        return []

    # Build a flat ordered list of all content blocks indexed by page_id
    # page_blocks: dict[int, list[block]]
    page_blocks: dict[int, list] = {}
    for page_block in rendered.children:
        pid = _page_id_from_block(page_block)
        blocks = []
        if page_block.children:
            for b in page_block.children:
                if b.block_type not in _SKIP_BLOCK_TYPES:
                    blocks.append(b)
        page_blocks[pid] = blocks

    # Classify and annotate ToC entries
    annotated = []
    for entry in toc:
        kind = classify_toc_entry(entry["title"])
        if kind is not None:
            annotated.append({
                "title": entry["title"].strip(),
                "page_id": entry["page_id"],
                "top_y": entry["polygon"][0][1],
                "kind": kind,
            })

    # Walk entries, tracking context
    current_poet = ""
    current_collection = ""
    poem_entries = []  # only 'poem'-kind entries with context attached

    for entry in annotated:
        if entry["kind"] == "poet":
            current_poet = _clean_name(entry["title"])
            current_collection = ""
        elif entry["kind"] == "collection":
            current_collection = entry["title"]
        else:
            poem_entries.append({
                **entry,
                "poet": current_poet,
                "collection": current_collection,
            })

    if not poem_entries:
        print("[warn] No poem entries found in ToC after classification.", file=sys.stderr)
        return []

    # Fallback: infer missing authors from page-level SectionHeader blocks
    if any(e["poet"] == "" for e in poem_entries):
        poem_entries = _infer_author_from_blocks(poem_entries, page_blocks)

    # Optionally merge speaker labels back into preceding poem
    if merge_speakers:
        poem_entries = _merge_speaker_labels(poem_entries)

    # For each poem entry, collect blocks between its start and the next entry's start
    poems: list[Poem] = []

    for i, entry in enumerate(poem_entries):
        poem_page = entry["page_id"]
        poem_top_y = entry["top_y"]

        if i + 1 < len(poem_entries):
            next_page = poem_entries[i + 1]["page_id"]
            next_top_y = poem_entries[i + 1]["top_y"]
        else:
            next_page = float("inf")
            next_top_y = float("inf")

        poem_text_parts = []
        first_page = None

        # Walk pages in order
        for pid in sorted(page_blocks.keys()):
            # Skip pages entirely before this poem
            if pid < poem_page:
                continue
            # Stop pages entirely after next poem
            if pid > next_page:
                break

            for block in page_blocks[pid]:
                top_y = _block_top_y(block)

                after_start = (
                    pid > poem_page
                    or (pid == poem_page and top_y >= poem_top_y)
                )
                before_end = (
                    pid < next_page
                    or (pid == next_page and top_y < next_top_y)
                )

                if after_start and before_end:
                    # Skip the SectionHeader that is the title itself
                    if block.block_type in _STRUCTURAL_BLOCK_TYPES and top_y == poem_top_y:
                        continue
                    text = _block_html_text(block)
                    if text:
                        # Render mid-poem section headers as markdown headings
                        # (e.g. sub-titles like "Charis' Triumph." within a poem)
                        if block.block_type == "SectionHeader":
                            m = re.match(r"<h([1-4])", block.html or "")
                            level = int(m.group(1)) if m else 2
                            poem_text_parts.append(f"{'#' * level} {text.strip()}")
                        else:
                            poem_text_parts.append(text)
                    if first_page is None:
                        first_page = pid

        poems.append(Poem(
            title=entry["title"],
            author=entry["poet"],
            collection=entry["collection"],
            source_anthology=anthology_name,
            source_page=first_page or poem_page,
            text="\n".join(poem_text_parts),
        ))

    return poems


def _infer_author_from_blocks(
    poem_entries: list[dict],
    page_blocks: dict[int, list],
) -> list[dict]:
    """
    Fallback: for poems where the ToC yielded no author, scan the SectionHeader
    blocks on the poem's page and the page before it.  If any block near the top
    of the page passes the same 'poet' heuristic used for ToC entries, treat it
    as the author header and propagate it forward until a new author is found.

    This handles the common anthology layout where:
      - an all-caps author name appears as a SectionHeader at the top of the
        first page of that author's section, and
      - every subsequent poem by the same author inherits that name.
    """
    # Build a per-page list of SectionHeader blocks sorted top→bottom
    def _poet_headers_on_page(pid: int) -> list[str]:
        headers = []
        for block in page_blocks.get(pid, []):
            if block.block_type != "SectionHeader":
                continue
            text = _block_html_text(block).strip()
            if classify_toc_entry(text) == "poet":
                headers.append((_block_top_y(block), text))
        # Return names only, sorted by y position (top first)
        return [name for _, name in sorted(headers)]

    result = []
    carry_poet = ""
    for entry in poem_entries:
        if entry["poet"]:
            carry_poet = entry["poet"]
            result.append(entry)
            continue

        # Try current page, then previous page
        found = ""
        for pid in (entry["page_id"], entry["page_id"] - 1):
            names = _poet_headers_on_page(pid)
            if names:
                found = _clean_name(names[0])
                break

        if found:
            carry_poet = found
            print(f"[author] Inferred '{carry_poet}' from page blocks for '{entry['title']}'",
                  file=sys.stderr)

        result.append({**entry, "poet": carry_poet})

    return result


def _merge_speaker_labels(poem_entries: list[dict]) -> list[dict]:
    """
    Merge ToC entries that are speaker labels (same page, within 200px of the
    previous poem, not the first entry) into the preceding poem's boundary.

    These appear in dramatic poems like masques where individual speakers
    ("Second Nymph.", "Third Nymph.") are marked as section headers but
    are part of the same work.
    """
    if len(poem_entries) < 2:
        return poem_entries

    merged = [poem_entries[0]]
    for entry in poem_entries[1:]:
        prev = merged[-1]
        same_page = entry["page_id"] == prev["page_id"]
        close_y = (entry["top_y"] - prev["top_y"]) < 200

        # Heuristic: speaker label = short (≤4 words), no [, same page, close y
        words = entry["title"].split()
        looks_like_speaker = (
            len(words) <= 4
            and not entry["title"].startswith("[")
            and same_page
            and close_y
        )

        if looks_like_speaker:
            # Swallow this entry — it's content within the previous poem.
            # Don't append; the boundary detection naturally includes its blocks.
            pass
        else:
            merged.append(entry)

    n_merged = len(poem_entries) - len(merged)
    if n_merged:
        print(f"[toc] Merged {n_merged} speaker label(s) into preceding poems.", file=sys.stderr)

    return merged


def _clean_name(raw: str) -> str:
    """Title-case and strip OCR artifacts from a poet/section name."""
    name = raw.strip().rstrip(".,;:")
    # Strip trailing page numbers
    name = re.sub(r"[\s.]*\d[\d\s]*$", "", name)
    # Fix OCR substitutions common in this series (e.g. yONSON → JONSON)
    name = re.sub(r"yO", "JO", name)
    # Remove caret OCR artefact
    name = name.replace("^", "").replace("\\", "")
    # Normalize whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name.title()


# ─────────────────────────────────────────────────────────────────────────────
# Marker runner
# ─────────────────────────────────────────────────────────────────────────────

def run_marker(pdf_path: str, start_page: int = None, end_page: int = None):
    """
    Run marker's PdfConverter with JSON renderer and return the rendered output.
    """
    from marker.converters.pdf import PdfConverter
    from marker.models import create_model_dict
    from marker.config.parser import ConfigParser

    register_block_class(BlockTypes.Line, PoetryLine)

    # Subclass PdfConverter to append StanzaBreakProcessor to the default pipeline.
    # processor_list expects classes (not instances), so we extend default_processors.
    class PoetryPdfConverter(PdfConverter):
        default_processors = PdfConverter.default_processors + (StanzaBreakProcessor,)

    config: dict = {"output_format": "json"}
    if start_page is not None:
        config["page_range"] = f"{start_page}-{end_page}" if end_page else f"{start_page}-"

    config_parser = ConfigParser(config)
    converter = PoetryPdfConverter(
        config=config_parser.generate_config_dict(),
        artifact_dict=create_model_dict(),
        renderer=config_parser.get_renderer(),
    )

    print(f"[marker] Converting {pdf_path} …", file=sys.stderr)
    return converter(pdf_path)


# ─────────────────────────────────────────────────────────────────────────────
# Output writers
# ─────────────────────────────────────────────────────────────────────────────

def write_output(poems: list[Poem], output_dir: Path, stem: str, fmt: str):
    output_dir.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        path = output_dir / f"{stem}_poems.csv"
        fields = ["title", "author", "collection", "source_anthology",
                  "source_page", "text", "notes", "needs_review", "review_reason"]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for poem in poems:
                writer.writerow({k: getattr(poem, k, "") for k in fields})
        print(f"[output] CSV → {path}")

    elif fmt == "json_combined":
        path = output_dir / f"{stem}_poems.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump([asdict(p) for p in poems], f, indent=2, ensure_ascii=False)
        print(f"[output] JSON → {path}")

    elif fmt == "json_per_poem":
        poem_dir = output_dir / stem
        poem_dir.mkdir(parents=True, exist_ok=True)
        for i, poem in enumerate(poems):
            slug = re.sub(r"[^\w]+", "_", poem.title.lower())[:50] or f"poem_{i}"
            path = poem_dir / f"{i:04d}_{slug}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(asdict(poem), f, indent=2, ensure_ascii=False)
        print(f"[output] Per-poem JSON → {poem_dir}/  ({len(poems)} files)")

    elif fmt == "markdown":
        poem_dir = output_dir / stem
        poem_dir.mkdir(parents=True, exist_ok=True)
        for i, poem in enumerate(poems):
            slug = re.sub(r"[^\w]+", "_", poem.title.lower())[:50] or f"poem_{i}"
            path = poem_dir / f"{i:04d}_{slug}.md"
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"# {poem.title}\n\n")
                if poem.author:
                    f.write(f"**Author:** {poem.author}  \n")
                if poem.collection:
                    f.write(f"**Collection:** {poem.collection}  \n")
                if poem.source_anthology:
                    f.write(f"**Source:** {poem.source_anthology}, p. {poem.source_page}  \n")
                f.write("\n---\n\n")
                f.write(poem.text)
                if poem.notes:
                    f.write(f"\n\n---\n\n*{poem.notes.strip()}*\n")
        print(f"[output] Markdown → {poem_dir}/  ({len(poems)} files)")

    elif fmt == "jsonl":
        path = output_dir / f"{stem}_poems.jsonl"
        with open(path, "w", encoding="utf-8") as f:
            for i, poem in enumerate(poems):
                slug = re.sub(r"[^\w]+", "_", f"{poem.author}_{poem.title}".lower()).strip("_")[:60]
                stanzas = [
                    [line for line in block.split("\n") if line.strip()]
                    for block in poem.text.split("\n\n")
                    if block.strip()
                ]
                lines = [line for stanza in stanzas for line in stanza]
                record = {
                    "id": f"{slug}_{i:04d}",
                    "title": poem.title,
                    "author": poem.author,
                    "collection": poem.collection,
                    "source_anthology": poem.source_anthology,
                    "source_page": poem.source_page,
                    "language": "en",
                    "source": "public_domain",
                    "text": poem.text,
                    "lines": lines,
                    "stanzas": stanzas,
                    "line_count": len(lines),
                    "stanza_count": len(stanzas),
                    "notes": poem.notes,
                    "needs_review": poem.needs_review,
                    "review_reason": poem.review_reason,
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        print(f"[output] JSONL → {path}  ({len(poems)} records)")

    else:
        print(f"[output] Unknown format '{fmt}', defaulting to csv", file=sys.stderr)
        write_output(poems, output_dir, stem, "csv")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Extract poems from anthology PDFs via Marker + ToC mapping."
    )
    parser.add_argument("--input", required=True,
                        help="Path to a PDF file or directory of PDFs")
    parser.add_argument("--output_dir", default="pipeline_output/marker_poems",
                        help="Output directory (default: pipeline_output/marker_poems)")
    parser.add_argument("--output_format", default="json_combined",
                        choices=["csv", "json_combined", "json_per_poem", "markdown", "jsonl"],
                        help="Output format (default: json_combined)")
    parser.add_argument("--start_page", type=int, default=None,
                        help="0-indexed start page (skip front matter)")
    parser.add_argument("--end_page", type=int, default=None,
                        help="0-indexed end page (exclusive)")
    parser.add_argument("--no_merge_speakers", action="store_true",
                        help="Disable merging of speaker labels into preceding poems")
    parser.add_argument("--dump_toc", action="store_true",
                        help="Print classified ToC entries and exit (useful for tuning)")

    args = parser.parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if input_path.is_file():
        pdfs = [input_path]
    elif input_path.is_dir():
        pdfs = sorted(input_path.glob("*.pdf"))
        if not pdfs:
            print(f"No PDFs found in {input_path}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Input not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    for pdf_path in pdfs:
        stem = pdf_path.stem
        try:
            rendered = run_marker(str(pdf_path), args.start_page, args.end_page)
        except Exception as e:
            print(f"[ERROR] marker failed on {pdf_path.name}: {e}", file=sys.stderr)
            continue

        if args.dump_toc:
            toc = rendered.metadata.get("table_of_contents", [])
            print(f"\n[toc] {len(toc)} entries in {pdf_path.name}:")
            for entry in toc:
                kind = classify_toc_entry(entry["title"]) or "SKIP"
                print(f"  {kind:<12}  p{entry['page_id']:>4}  {entry['title']!r}")
            continue

        poems = map_toc_to_poems(
            rendered,
            anthology_name=stem,
            merge_speakers=not args.no_merge_speakers,
        )

        print(f"[extract] {len(poems)} poems from {pdf_path.name}")
        review_count = sum(1 for p in poems if p.needs_review)
        if review_count:
            print(f"[review]  {review_count} poems flagged for review")

        write_output(poems, output_dir, stem, args.output_format)

    print("Done.")


if __name__ == "__main__":
    main()
