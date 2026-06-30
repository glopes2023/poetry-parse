# Poetry Parse

Extracts individual poems from poetry anthology PDFs using [Marker](https://github.com/VikParuchuri/marker)'s structured JSON output and table of contents mapping.

## How it works

- Uses Marker's `PdfConverter` to extract text blocks with spatial metadata
- Maps ToC entries to poems using polygon-based boundary detection
- Classifies ToC entries as poet, collection, or poem by heuristic
- Falls back to page-level section headers for author attribution when the ToC doesn't include them
- Strips page headers, footers, and footnotes from poem text
- Merges speaker labels in dramatic poems back into the preceding poem

## Requirements

- Python 3.11+
- [marker-pdf](https://github.com/VikParuchuri/marker)

## Setup

```bash
# Create and activate a virtual environment
python3 -m venv marker-env
source marker-env/bin/activate

# Install marker
pip install marker-pdf
```

## Tutorial: parse and evaluate an anthology

This walks through the full workflow for a single anthology — extracting the
poems, then scoring the extraction against a reference table of contents.

### 1. Parse the PDF

**Find the poem page range.** Open the PDF and note the first and last pages
that contain poem content, skipping front matter (title page, preface, ToC) and
back matter (index, notes). These are 0-indexed and the end is exclusive, so if
the poems run from the 29th through the 38th page of the file, use
`--start_page 28 --end_page 38`.

**Create a config sidecar** (optional, but recommended). Place a JSON file next
to the PDF with the same stem (e.g. `anthology.pdf` → `anthology.json`). The most
common setting tells the parser whether each poet's name is printed in the
running page headers:

```json
{
  "author_in_page_headers": true
}
```

See [Per-anthology configuration](#per-anthology-configuration) for all
available fields. If no sidecar is needed, the defaults apply.

**Run the parser:**

```bash
python parse_marker.py --input data/anthology.pdf --start_page 28 --end_page 38
```

This writes the extracted poems to `pipeline_output/marker_poems/anthology_poems.json`
(the file stem matches the PDF). Override the location with `--output_dir`.

### 2. Evaluate accuracy

Compare the extracted poems against a reference ToC to flag missing poems,
page mismatches, and wrong author attributions.

You need a reference CSV with columns `title, page [, author]` (author optional).

```bash
python compare_toc.py \
  --pdf data/anthology.pdf \
  --toc-file data/anthology_toc.csv \
  --page-offset 19
```

- `--pdf` — the same PDF. The script locates the extraction by file stem,
  reading `anthology_poems.json` from the output directory (`--output-dir`,
  default `pipeline_output/marker_poems`).
- `--toc-file` — the reference ToC CSV.
- `--page-offset` — the gap between the page numbers *printed in the book* and
  the *actual page count from the start of the PDF*. Find the PDF page where the
  printed page "1" appears; if that's the 20th page of the file, the offset is
  19. The offset is added to every reference page number before alignment.

It prints a summary and fidelity score to the terminal and writes a review
sheet to `pipeline_output/marker_poems/anthology_toc_review.csv`. The reference
flags (`MISSING`, `EXTRA`, etc.) are only produced when `--toc-file` is given;
without it, only the structural flags are computed.

#### Review CSV fields

One row per extracted poem, plus one trailing row per reference entry that was
never matched (flagged `MISSING`). Rows are sorted by page.

| Column | Description |
|---|---|
| `idx` | Index of the poem in the extraction (blank for `MISSING` rows) |
| `ref_title` | Title from the reference ToC (blank if the poem matched nothing) |
| `ref_page` | Page from the reference ToC, after `--page-offset` is applied |
| `parser_title` | Title the parser extracted |
| `parser_page` | `source_page` the parser assigned |
| `author` | Author the parser assigned |
| `ref_author` | Expected author from the ToC author timeline at this page |
| `author_source` | How the parser derived the author (e.g. ToC carry-forward, section header, page header) |
| `text_length` | Character count of the extracted poem text |
| `flags` | Pipe-separated list of flags raised for this row (see below) |
| `text_preview` | First 120 characters of the poem text, newlines flattened |
| `manual_author` | Empty column for hand-correcting the author during review |

#### Flags

**Structural** (always computed, no reference needed):

| Flag | Meaning |
|---|---|
| `NO_AUTHOR` | Author field is empty |
| `AUTHOR_SUSPECT` | Author looks like a title/section header, not a person — a roman numeral, a `Book/Canto N` label, or a string that matches one of the poem titles |
| `SHORT_TEXT` | Poem text is under 80 characters (likely a fragment or a mis-split) |
| `SWALLOWED` | Text is abnormally long (> Q3 + 3×IQR of all poem lengths) or contains embedded `##` headings — a sign that a boundary was missed and the next poem was absorbed |
| `PAGE_GAP` | `source_page` jumped more than 15 pages from the previous poem (possible skipped content) |

**Reference** (only with `--toc-file`):

| Flag | Meaning |
|---|---|
| `MISSING` | A reference entry has no matching extracted poem (under-extraction) |
| `EXTRA` | An extracted poem matches no reference entry (over-extraction or spurious record) |
| `PAGE_MISMATCH` | Title matched a reference entry but the pages differ by more than 3 |
| `AUTHOR_MISMATCH` | Author is a plausible name but disagrees with the ToC author timeline at this page; the expected name is in `ref_author` |

Titles are aligned to the reference in three passes: exact normalized match,
character-level fuzzy match (`SequenceMatcher ≥ 0.80`), then word-overlap
(`≥ 0.65`) plus page proximity — so OCR noise like `Exho's` vs `Echo's` still
matches.

#### Fidelity score

The terminal output ends with a fidelity breakdown. Three structural sub-scores
are always shown (each is `1 − flagged/total`):

- **Boundary quality** = `1 − SWALLOWED/N` — how cleanly poems were split
- **Text completeness** = `1 − SHORT_TEXT/N` — how few suspiciously short poems
- **Author coverage** = `1 − NO_AUTHOR/N` — how often an author was assigned

With a reference ToC, three more are added by matching extracted poems to ref
entries:

- **Recall** = matched / reference entries (coverage — did we find everything?)
- **Precision** = matched / extracted poems (did we avoid spurious records?)
- **F1** = harmonic mean of recall and precision

The single **Fidelity score** combines these:

- With `--toc-file`: `0.5 × F1 + 0.3 × boundary + 0.2 × author_coverage`
- Without: `0.6 × boundary + 0.4 × author_coverage`

Use the score for an at-a-glance pass/fail and the flagged rows in the CSV to
find and fix the specific problems — often by adjusting the page range or the
[config sidecar](#per-anthology-configuration) and re-parsing.

## Usage

Place PDFs in a `data/` directory, then run:

```bash
# Single PDF, default output (json_combined)
python parse_marker.py --input data/anthology.pdf

# Specific page range (0-indexed)
python parse_marker.py --input data/anthology.pdf --start_page 28 --end_page 38

# Output as markdown (one file per poem)
python parse_marker.py --input data/anthology.pdf --output_format markdown

# All PDFs in a directory
python parse_marker.py --input data/ --output_format csv

# Inspect ToC classification before parsing
python parse_marker.py --input data/anthology.pdf --dump_toc
```

## Corpus post-processing

> ⚠️ **Experimental — not fully fleshed out or tested.** The corpus pipeline is
> a work in progress and hasn't been validated end-to-end. For now, prefer the
> primary workflow above (parse with `parse_marker.py`, evaluate with
> `compare_toc.py`). Treat anything below as exploratory.

After extraction, run the corpus pipeline to validate records, quarantine likely
structural problems, and produce OCR-noise candidates:

```bash
python corpus_pipeline.py --input pipeline_output/marker_poems
```

This writes to `pipeline_output/corpus/` by default:

| File | Description |
|---|---|
| `clean_poems.jsonl` | Records without error-level QA flags |
| `review_poems.jsonl` | Records needing review, such as empty text or swallowed boundaries |
| `review_poems.csv` | Compact review sheet with IDs, flags, metrics, and text previews |
| `final_poems.jsonl` | Analysis-ready records after automatic QA and optional review decisions |
| `ocr_candidates.csv` | Suspicious OCR tokens with example context |
| `corpus_report.json` | Summary counts by anthology, flag type, and severity |

The pipeline preserves parser fields and adds `id`, `text_raw`, `text_clean`,
`qa_flags`, `qa_flag_codes`, `source_record_index`, and `source_record_path`.
It treats JSON and JSONL outputs with the same anthology stem as alternate
serializations and loads only one, avoiding false duplicate reports.

To create a review-decision template:

```bash
python corpus_pipeline.py --input pipeline_output/marker_poems --write_decisions_template
```

This creates `pipeline_output/corpus/corpus_decisions.template.json` with the
current review IDs. Copy or rename it, then fill `keep_ids`, `drop_ids`, and
optional `notes`. Re-run with:

```bash
python corpus_pipeline.py \
  --input pipeline_output/marker_poems \
  --decisions pipeline_output/corpus/corpus_decisions.json
```

Without a decisions file, `final_poems.jsonl` contains only automatically clean
records. Review records move into `final_poems.jsonl` only when their IDs are
listed in `keep_ids`.

## Output formats

| Format | Description |
|---|---|
| `json_combined` | Single JSON file with all poems (default) |
| `json_per_poem` | One JSON file per poem |
| `markdown` | One `.md` file per poem |
| `csv` | Single CSV with all poems |
| `jsonl` | One JSON Lines record per poem |

Output is written to `pipeline_output/marker_poems/` by default. Override with `--output_dir`.

## Options

| Flag | Description |
|---|---|
| `--input` | Path to a PDF file or directory of PDFs (required) |
| `--output_dir` | Output directory (default: `pipeline_output/marker_poems`) |
| `--output_format` | Output format (default: `json_combined`) |
| `--start_page` | 0-indexed start page |
| `--end_page` | 0-indexed end page (exclusive) |
| `--no_merge_speakers` | Disable merging of speaker labels into preceding poems |
| `--dump_toc` | Print classified ToC entries and exit |

## Per-anthology configuration

Config is loaded via a two-level lookup, so you can set a baseline for an entire directory and override it per-file as needed:

1. **Directory-level** — place a `config.json` in the same directory as the PDFs. Applies to every PDF in that directory.
2. **Per-file sidecar** — a JSON file with the same stem as the PDF (e.g. `anthology.pdf` → `anthology.json`). Any keys here override the directory-level values.

If neither file exists, all defaults apply.

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `author_position` | string | `"before_poems"` | Where author attribution appears relative to poem content (see below) |
| `author_attribution_pattern` | string or null | `null` | Regex for extracting author from a trailing attribution block |
| `poet_uppercase_threshold` | float | `0.65` | Minimum uppercase fraction for a ToC entry to be classified as a poet header |
| `author_in_page_headers` | bool | `false` | Scan running page headers for poet names as a last-resort author fallback |
| `skip_bio_pattern` | string or null | `null` | Regex to drop biography/introduction sections from the ToC before extraction |

### `author_position`

Controls how author attribution is detected:

- **`"before_poems"`** (default) — one all-caps author header in the ToC covers a section of poems. Standard anthology layout.
- **`"after_poem"`** — an attribution block appears immediately after each poem's text (before the next poem title). The block is extracted from the poem text and stored as the author.

### `author_attribution_pattern`

Only used when `author_position` is `"after_poem"`. A regex applied with `re.match()` to the candidate trailing block. If capture group 1 is present it is used as the author name; otherwise the full match is used. Useful for anthologies that use punctuated attributions like `"— Ben Jonson"`.

When `null` (default), the standard all-caps uppercase-ratio heuristic is used instead.

### `poet_uppercase_threshold`

A short ToC entry is classified as a poet header when the fraction of alphabetic characters that are uppercase meets or exceeds this value. Lower it for anthologies whose author names are printed in mixed case (e.g. `0.4`).

### `author_in_page_headers`

When `true`, PageHeader blocks (the running headers printed at the top of pages) are scanned for poet names and used as a last-resort author fallback — after the ToC carry-forward and SectionHeader scan have both failed. Enable this for anthologies that print each poet's name as a repeating page header throughout their section, especially when a multi-page biography separates the author heading from the first poem.

### `skip_bio_pattern`

A case-insensitive regex matched against ToC entry titles. Poem-type entries whose title matches are dropped before extraction, preventing biography or introduction sections from becoming spurious poem records. Only `poem`-classified entries are filtered; `poet` and `collection` entries are never dropped.

### Examples

Basic after-poem attribution with a dash prefix:

```json
{
  "author_position": "after_poem",
  "author_attribution_pattern": "^[-–—]\\s*(.+)$"
}
```

Anthology with multi-page biographies and author names in running page headers:

```json
{
  "author_in_page_headers": true,
  "skip_bio_pattern": "^(Life of|Memoir of|Introduction to)"
}
```

Mixed-case author names with biography sections to skip:

```json
{
  "poet_uppercase_threshold": 0.4,
  "skip_bio_pattern": "^(Note on|Notes on|Editor.s Note)"
}
```
