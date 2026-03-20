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
