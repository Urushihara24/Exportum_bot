# Project Structure

## Runtime files
- `main.py` - main bot entrypoint.
- `.env` - local runtime secrets (not for sharing).
- `.env.example` - safe environment template.
- `requirements.txt` - pinned dependencies.
- `run.sh` - local start script.

## Runtime directories
- `data/` - application state (`*.pkl` and optional JSON snapshots).
- `logs/` - runtime logs and archived logs.

## Documentation
- `README.md` - product and usage documentation.
- `docs/README.md` - docs index.
- `docs/reports_2026-02-16.tar.gz` - compressed audit/testing reports bundle.
- `docs/notes/` - technical notes and TODO lists.

## Archives
- `archive/` - backups, scratch files, and old datasets moved from root.

## Hygiene rules
- Keep only runtime-critical files in project root.
- Move one-off scripts and backups into `archive/`.
- Keep reports in a compressed bundle under `docs/`.
- Do not store real tokens in source code.
