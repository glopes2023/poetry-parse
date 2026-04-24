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

Per-anthology configuration:
  Each anthology can have a sidecar JSON config file placed next to the PDF
  with the same stem (e.g. anthology.pdf → anthology.json).  If found, it
  overrides the default parsing behaviour for that file only.

  Supported fields:

    author_position (str, default "before_poems")
      Controls where author attribution is expected relative to poem content.
        "before_poems"  — one all-caps author header in the ToC covers a
                          section of poems.  This is the default.
        "after_poem"    — attribution appears as a block immediately after
                          each poem's text (before the next poem title).
                          The block is extracted from the poem text and
                          stored as the author.

    author_attribution_pattern (str | null, default null)
      Optional regex applied to candidate author blocks when
      author_position == "after_poem".  If provided it takes precedence
      over the standard all-caps poet heuristic, allowing anthologies
      that use title-case or punctuated attributions (e.g. "— Ben Jonson")
      to be handled correctly.  The full stripped block text is matched
      with re.match(); capture group 1, if present, is used as the name
      (otherwise the whole match is used).

    poet_uppercase_threshold (float, default 0.65)
      Minimum fraction of alphabetic characters that must be uppercase for
      a short ToC entry to be classified as a poet header rather than a
      poem title.  Lower this value for anthologies whose author names
      are printed in mixed case.

    author_in_page_headers (bool, default false)
      When true, PageHeader blocks (the running headers printed at the top
      of alternating pages) are scanned for poet names and used as a
      last-resort author fallback.  Enable this for anthologies that print
      each poet's name as a repeating page header throughout their section,
      particularly when a multi-page biography separates the author heading
      from their first poem.

    skip_bio_pattern (str | null, default null)
      Regex (case-insensitive) matched against ToC entry titles.  Poem-type
      entries whose title matches are dropped before extraction, preventing
      biography or introduction sections that appear in the ToC from
      becoming spurious poem records.
      Example: "^(Life of|Biography|Introduction to|Memoir of)"

  Example sidecar file (anthology.json):
    {
      "author_position": "after_poem",
      "author_attribution_pattern": "^[-–—]\\s*(.+)$"
    }

  Example for bio-section anthologies:
    {
      "author_in_page_headers": true,
      "skip_bio_pattern": "^(Life of|Memoir of|Introduction to)"
    }

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


@dataclass
class AnthologyConfig:
    """
    Per-anthology parsing configuration loaded from a sidecar JSON file.

    See the module docstring for full field documentation and an example file.
    Instantiate directly for programmatic use, or call load_anthology_config()
    to auto-discover the sidecar file for a given PDF path.
    """
    author_position: str = "before_poems"
    """
    Where author attribution appears relative to poem content.
    "before_poems" (default) — all-caps author header precedes a section.
    "after_poem"              — attribution block follows each poem's text.
    """

    author_attribution_pattern: Optional[str] = None
    """
    Regex matched against candidate attribution blocks when
    author_position == "after_poem".  Capture group 1 (if present) is
    extracted as the author name; otherwise the full match is used.
    When None the standard uppercase-ratio heuristic is used instead.
    """

    poet_uppercase_threshold: float = 0.65
    """
    Minimum fraction of uppercase alphabetic characters required for a
    short ToC entry to be classified as a poet header.  Default 0.65.
    """

    author_in_page_headers: bool = False
    """
    When True, PageHeader blocks (the running headers printed at the top of
    alternating pages) are scanned for author names.  Useful for anthologies
    that print each poet's name as a repeating page header throughout their
    section, especially when a multi-page biography separates the author
    heading from the first poem.

    Author names found this way are used as a last-resort fallback after the
    ToC carry-forward and the SectionHeader block scan have both failed to
    identify an author for a poem entry.  The highest-priority match on the
    poem's page is used; if none, the preceding page is tried.
    """

    skip_bio_pattern: Optional[str] = None
    """
    Optional regex applied to every ToC entry title after classification.
    Entries whose title matches (re.search) are dropped before poem
    extraction, preventing multi-page biography sections from becoming
    spurious poem records.

    The pattern is matched case-insensitively.  Common values:
        "^(Life of|Biography|Introduction to|Memoir of)"
        "^(Note on|Notes on|Editor.s Note)"
    Only "poem"-classified entries are filtered; "poet" and "collection"
    entries are never dropped (they carry no text of their own).
    """


# ─────────────────────────────────────────────────────────────────────────────
# ToC classification
# ─────────────────────────────────────────────────────────────────────────────

# Genre/form keywords that look all-caps but are poem-type headings, not poet names
_GENRE_WORDS = {
    "SONG", "SONGS", "ODE", "ODES", "HYMN", "HYMNS", "SONNET", "SONNETS",
    "EPIGRAM", "EPIGRAMS", "ELEGY", "ELEGIES", "EPISTLE", "EPISTLES",
    "SATIRE", "SATIRES", "BALLAD", "BALLADS", "PROLOGUE", "EPILOGUE",
    "CHORUS", "DIRGE", "EPITAPH", "EPITAPHS", "MADRIGAL", "SEXTAIN",
    "TRAGEDY", "TRAGEDIES", "COMEDY", "COMEDIES", "DRAMA", "MASQUE", "MASQUES",
}


def classify_toc_entry(title: str, threshold: float = 0.65) -> Optional[str]:
    """
    Return 'poet', 'collection', or 'poem' for a ToC title, or None to skip.

    Rules (applied in order):
      1. Empty / whitespace-only  → None (skip)
      2. Starts with '['          → 'collection'  (e.g. "[From The Forest.]")
      3. All-caps, ≤6 words, not a genre word  → 'poet'
      4. Anything else            → 'poem'

    Parameters
    ----------
    title : str
        Raw title string from the ToC or a SectionHeader block.
    threshold : float
        Minimum fraction of alphabetic characters that must be uppercase for
        rule 3 to apply.  Defaults to 0.65; lower for mixed-case anthologies.
        Comes from AnthologyConfig.poet_uppercase_threshold.
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
            if upper_ratio >= threshold:
                # Exclude genre/form words that happen to be all-caps
                cleaned = re.sub(r"[^A-Za-z\s]", "", t).strip().upper()
                cleaned_words = cleaned.split()
                if not any(w in _GENRE_WORDS or w.rstrip("S") in _GENRE_WORDS for w in cleaned_words):
                    # Exclude common poem-starter words that can appear in short all-caps lines
                    poem_starters = {"AND", "THE", "BUT", "OR", "IF", "TO", "IN",
                                     "AS", "THAT", "WITH", "FOR", "NOT", "ALL",
                                     "MY", "HER", "HIS", "ON", "WHERE", "WHEN",
                                     "FROM", "UPON", "AGAINST", "AFTER", "BEFORE"}
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
    html = re.sub(r"<br\s*/?>", "\n", block.html or "", flags=re.IGNORECASE)
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
    config: Optional[AnthologyConfig] = None,
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
    config : AnthologyConfig, optional
        Per-anthology parsing settings (author position, thresholds, etc.).
        If None a default AnthologyConfig() is used, which is equivalent to
        the previous behaviour.
    """
    if config is None:
        config = AnthologyConfig()
    toc = rendered.metadata.get("table_of_contents", [])
    if not toc:
        print("[warn] table_of_contents is empty — no poems extracted.", file=sys.stderr)
        return []

    # Build a flat ordered list of all content blocks indexed by page_id.
    # PageHeader / PageFooter / Footnote are stripped here so they never
    # appear in poem text.  When author_in_page_headers is set we do a
    # separate pre-pass below (before stripping) to harvest author names.
    page_blocks: dict[int, list] = {}
    for page_block in rendered.children:
        pid = _page_id_from_block(page_block)
        blocks = []
        if page_block.children:
            for b in page_block.children:
                if b.block_type not in _SKIP_BLOCK_TYPES:
                    blocks.append(b)
        page_blocks[pid] = blocks

    # Optional: build page_id → author map from running PageHeader blocks.
    page_header_authors: dict[int, str] = {}
    if config.author_in_page_headers:
        page_header_authors = _build_page_header_author_map(
            rendered, threshold=config.poet_uppercase_threshold
        )
        if page_header_authors:
            print(
                f"[author] Found page-header authors on "
                f"{len(page_header_authors)} page(s).",
                file=sys.stderr,
            )

    # Classify and annotate ToC entries
    annotated = []
    for entry in toc:
        kind = classify_toc_entry(entry["title"], threshold=config.poet_uppercase_threshold)
        if kind is not None:
            annotated.append({
                "title": entry["title"].strip(),
                "page_id": entry["page_id"],
                "top_y": entry["polygon"][0][1],
                "kind": kind,
            })

    # Walk entries, tracking context.
    # "poem"-kind entries that match skip_bio_pattern are dropped before
    # being added to poem_entries — this prevents biography sections that
    # appear in the ToC from becoming spurious poem records.
    bio_re = (
        re.compile(config.skip_bio_pattern, re.IGNORECASE)
        if config.skip_bio_pattern
        else None
    )
    current_poet = ""
    current_collection = ""
    poem_entries = []  # only 'poem'-kind entries with context attached
    n_skipped_bio = 0

    # Words that signal a continuation of the previous poet name (e.g. "OF HAWTHORNDEN")
    _POET_CONTINUATIONS = {
        "OF", "DE", "VAN", "DU", "DI", "VON", "LA", "LE",
        "EARL", "LORD", "COUNT", "DUKE", "BARON", "VISCOUNT", "MARQUESS",
    }

    for entry in annotated:
        if entry["kind"] == "poet":
            first_word = entry["title"].strip().split()[0].upper().strip(".,;:")
            if current_poet and first_word in _POET_CONTINUATIONS:
                current_poet = current_poet + " " + _clean_name(entry["title"])
            else:
                current_poet = _clean_name(entry["title"])
                current_collection = ""
        elif entry["kind"] == "collection":
            current_collection = entry["title"]
        else:
            if bio_re and bio_re.search(entry["title"]):
                n_skipped_bio += 1
                continue
            poem_entries.append({
                **entry,
                "poet": current_poet,
                "collection": current_collection,
            })

    if n_skipped_bio:
        print(
            f"[toc] Skipped {n_skipped_bio} bio/intro section(s) "
            f"matching skip_bio_pattern.",
            file=sys.stderr,
        )

    if not poem_entries:
        print("[warn] No poem entries found in ToC after classification.", file=sys.stderr)
        return []

    # Fallback: infer missing authors from page-level SectionHeader blocks,
    # and (if author_in_page_headers) from the page-header author map.
    # Only run for before_poems convention; after_poem handles attribution
    # during block collection below.
    if config.author_position == "before_poems" and any(e["poet"] == "" for e in poem_entries):
        poem_entries = _infer_author_from_blocks(
            poem_entries,
            page_blocks,
            threshold=config.poet_uppercase_threshold,
            page_header_authors=page_header_authors,
        )

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
        collected_blocks = []  # (block, rendered_text) — used for after_poem extraction
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
                            rendered_text = f"{'#' * level} {text.strip()}"
                        else:
                            rendered_text = text
                        poem_text_parts.append(rendered_text)
                        collected_blocks.append((block, rendered_text))
                    if first_page is None:
                        first_page = pid

        # For after_poem anthologies: extract the trailing attribution block
        # (if any) and use it as the author rather than leaving it in the text.
        author = entry["poet"]
        if config.author_position == "after_poem" and collected_blocks:
            inferred, poem_text_parts = _extract_trailing_author(
                collected_blocks, poem_text_parts, config
            )
            if inferred:
                author = inferred
                print(
                    f"[author] Extracted trailing author '{author}' "
                    f"for '{entry['title']}'",
                    file=sys.stderr,
                )

        poems.append(Poem(
            title=entry["title"],
            author=author,
            collection=entry["collection"],
            source_anthology=anthology_name,
            source_page=first_page or poem_page,
            text="\n".join(poem_text_parts),
        ))

    return poems


def _build_page_header_author_map(rendered, threshold: float = 0.65) -> dict[int, str]:
    """
    Pre-pass over all pages to build a mapping of page_id → author_name from
    PageHeader blocks.

    PageHeader blocks are ordinarily stripped from poem text, but for anthologies
    that print each poet's name as a running page header throughout their section
    (author_in_page_headers=True) they are the most reliable author signal —
    especially when a multi-page biography separates the author heading from the
    first poem and the ToC carries no poet entry for that gap.

    Only headers that pass the standard classify_toc_entry "poet" heuristic
    (uppercase-ratio test) are recorded.  If multiple poet-like headers appear on
    the same page the first one (lowest y) wins.

    Parameters
    ----------
    rendered : JSONOutput
        The raw PdfConverter output (has .children for pages).
    threshold : float
        Passed through to classify_toc_entry; comes from
        AnthologyConfig.poet_uppercase_threshold.

    Returns
    -------
    dict[int, str]
        Maps page_id to cleaned author name for every page that has a
        poet-classified PageHeader block.
    """
    result: dict[int, str] = {}
    for page_block in rendered.children:
        pid = _page_id_from_block(page_block)
        if not page_block.children:
            continue
        candidates = []
        for block in page_block.children:
            if block.block_type != "PageHeader":
                continue
            text = _block_html_text(block).strip()
            if classify_toc_entry(text, threshold=threshold) == "poet":
                candidates.append((_block_top_y(block), text))
        if candidates:
            # Take the topmost header on the page
            _, name = min(candidates, key=lambda x: x[0])
            result[pid] = _clean_name(name)
    return result


def _infer_author_from_blocks(
    poem_entries: list[dict],
    page_blocks: dict[int, list],
    threshold: float = 0.65,
    page_header_authors: Optional[dict[int, str]] = None,
) -> list[dict]:
    """
    Fallback: for poems where the ToC yielded no author, scan the SectionHeader
    blocks on the poem's page and the page before it.  If any block near the top
    of the page passes the same 'poet' heuristic used for ToC entries, treat it
    as the author header and propagate it forward until a new author is found.

    If page_header_authors is provided (from _build_page_header_author_map), it
    is consulted as a last resort after the SectionHeader scan fails.  This
    covers the bio-section layout where the poet's name only appears in running
    page headers and not as a SectionHeader block on the poem's own page.

    This handles the common anthology layouts where:
      - an all-caps author name appears as a SectionHeader at the top of the
        first page of that author's section, and
      - every subsequent poem by the same author inherits that name; or
      - a multi-page biography separates the author heading from the first poem,
        but the author's name repeats in the page header throughout their section.

    Parameters
    ----------
    poem_entries : list[dict]
        Annotated poem entries from map_toc_to_poems.
    page_blocks : dict[int, list]
        All content blocks keyed by page ID.
    threshold : float
        Passed through to classify_toc_entry; comes from
        AnthologyConfig.poet_uppercase_threshold.
    page_header_authors : dict[int, str], optional
        Map of page_id → author_name built from PageHeader blocks.  Only
        provided when AnthologyConfig.author_in_page_headers is True.
    """
    if page_header_authors is None:
        page_header_authors = {}

    # Build a per-page list of SectionHeader blocks sorted top→bottom
    def _poet_headers_on_page(pid: int) -> list[str]:
        headers = []
        for block in page_blocks.get(pid, []):
            if block.block_type != "SectionHeader":
                continue
            text = _block_html_text(block).strip()
            if classify_toc_entry(text, threshold=threshold) == "poet":
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

        # Priority 1: SectionHeader blocks on current page, then previous page
        found = ""
        source = ""
        for pid in (entry["page_id"], entry["page_id"] - 1):
            names = _poet_headers_on_page(pid)
            if names:
                found = _clean_name(names[0])
                source = "section header"
                break

        # Priority 2: running PageHeader blocks (author_in_page_headers path)
        if not found:
            for pid in (entry["page_id"], entry["page_id"] - 1):
                if pid in page_header_authors:
                    found = page_header_authors[pid]
                    source = "page header"
                    break

        if found:
            carry_poet = found
            print(
                f"[author] Inferred '{carry_poet}' from {source} "
                f"for '{entry['title']}'",
                file=sys.stderr,
            )

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

        # Heuristic: speaker label = short (≤4 words), no [, same page, close y,
        # and title contains no genre/form words (e.g. "Ode", "Song", "Hymn")
        words = entry["title"].split()
        title_words = {re.sub(r"[^A-Za-z]", "", w).upper() for w in words}
        contains_genre_word = any(
            w in _GENRE_WORDS or w.rstrip("S") in _GENRE_WORDS for w in title_words
        )
        looks_like_speaker = (
            len(words) <= 4
            and not entry["title"].startswith("[")
            and not contains_genre_word
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


def _extract_trailing_author(
    collected_blocks: list[tuple],
    poem_text_parts: list[str],
    config: AnthologyConfig,
) -> tuple[str, list[str]]:
    """
    For the "after_poem" convention: inspect the last collected block to see
    if it is an author attribution rather than poem body.  If it matches,
    return the cleaned author name and the text-parts list with that block
    removed.  If it does not match, return ("", poem_text_parts) unchanged.

    Matching strategy (in priority order):
      1. If config.author_attribution_pattern is set, apply it with re.match.
         Capture group 1, if present, is used as the name; otherwise the full
         match text is used.
      2. Otherwise fall back to the standard classify_toc_entry "poet"
         heuristic (uppercase-ratio test) with the configured threshold.

    Parameters
    ----------
    collected_blocks : list[tuple]
        (block, rendered_text) pairs in document order for the current poem.
    poem_text_parts : list[str]
        Rendered text strings in the same order — the last entry corresponds
        to the last block.
    config : AnthologyConfig
        The anthology's configuration; provides author_attribution_pattern
        and poet_uppercase_threshold.
    """
    if not collected_blocks:
        return "", poem_text_parts

    _, last_text = collected_blocks[-1]
    candidate = last_text.strip()

    if config.author_attribution_pattern:
        m = re.match(config.author_attribution_pattern, candidate)
        if m:
            # Use first capture group if present, else the full match
            raw_name = m.group(1) if m.lastindex and m.lastindex >= 1 else m.group(0)
            return _clean_name(raw_name), poem_text_parts[:-1]
        return "", poem_text_parts

    # Fallback: standard uppercase-ratio heuristic
    if classify_toc_entry(candidate, threshold=config.poet_uppercase_threshold) == "poet":
        return _clean_name(candidate), poem_text_parts[:-1]

    return "", poem_text_parts


def load_anthology_config(pdf_path: Path) -> AnthologyConfig:
    """
    Load config for the given PDF using a two-level lookup:

      1. Directory-level: config.json in the same directory as the PDF.
         Applies to every PDF in that directory — useful for large batches
         that share a common layout.
      2. Per-file sidecar: a JSON file with the same stem as the PDF
         (e.g. anthology.pdf → anthology.json).  Any keys present here
         override the directory-level values.

    If neither file exists, a default AnthologyConfig() is returned.
    Unknown keys in either file are silently ignored.
    """
    valid_fields = AnthologyConfig.__dataclass_fields__
    merged: dict = {}

    dir_config_path = pdf_path.parent / "config.json"
    if dir_config_path.exists():
        with open(dir_config_path, encoding="utf-8") as f:
            data = json.load(f)
        kwargs = {k: v for k, v in data.items() if k in valid_fields}
        merged.update(kwargs)
        print(f"[config] Loaded directory config from {dir_config_path.name}: {kwargs}", file=sys.stderr)

    per_file_path = pdf_path.with_suffix(".json")
    if per_file_path.exists():
        with open(per_file_path, encoding="utf-8") as f:
            data = json.load(f)
        kwargs = {k: v for k, v in data.items() if k in valid_fields}
        merged.update(kwargs)
        print(f"[config] Loaded per-file config from {per_file_path.name}: {kwargs}", file=sys.stderr)

    return AnthologyConfig(**merged)


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
        config["page_range"] = f"{start_page}-{end_page}" if end_page is not None else f"{start_page}-"

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
                  "source_page", "text", "notes"]
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
        anthology_config = load_anthology_config(pdf_path)

        try:
            rendered = run_marker(str(pdf_path), args.start_page, args.end_page)
        except Exception as e:
            print(f"[ERROR] marker failed on {pdf_path.name}: {e}", file=sys.stderr)
            continue

        if args.dump_toc:
            toc = rendered.metadata.get("table_of_contents", [])
            print(f"\n[toc] {len(toc)} entries in {pdf_path.name}:")
            for entry in toc:
                kind = classify_toc_entry(
                    entry["title"],
                    threshold=anthology_config.poet_uppercase_threshold,
                ) or "SKIP"
                print(f"  {kind:<12}  p{entry['page_id']:>4}  {entry['title']!r}")
            continue

        poems = map_toc_to_poems(
            rendered,
            anthology_name=stem,
            merge_speakers=not args.no_merge_speakers,
            config=anthology_config,
        )

        print(f"[extract] {len(poems)} poems from {pdf_path.name}")

        write_output(poems, output_dir, stem, args.output_format)

    print("Done.")


if __name__ == "__main__":
    main()
