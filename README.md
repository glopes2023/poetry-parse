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
