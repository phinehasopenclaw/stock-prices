# Scanner archive

One folder per trading day, written by scan.py at 15:05 CT:

- `signals.json`: every signal that day, with entry, stop, targets, relative volume, catalyst and the scored outcome.
- `board.jsonl.gz`: the scanner board once a minute (setups, levels, volume) plus which names were armed.
- `breaks.jsonl.gz`: every armed level the price reached, and whether it broke cleanly, wicked and rejected, or was already past the chase limit.
- `bars_1m.csv.gz`: 1-minute bars (pre-market through close) for every name that appeared that day.

Use these to score real fills against what the scanner showed at the time.

2026-11-13 is partial: the per-minute board history started at about 10:50 CT, when archiving was added.
`signals_through_1035.json`, `board_snapshot_1035.json` and `latency_study_nov12-13.csv` were saved by hand.
