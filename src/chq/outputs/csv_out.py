"""CSV output formatter for chq reports."""

from __future__ import annotations

import csv
import sys


def output_csv(results: list, config) -> None:
    """Write *results* as CSV to a file or stdout.

    Each :class:`QueryResult` is preceded by a comment line
    ``# category/name``, followed by a header row and data rows, then a
    blank line separator.
    """
    output_path = getattr(config, "output_path", None)

    if output_path:
        fh = open(output_path, "w", newline="", encoding="utf-8")  # noqa: SIM115
    else:
        fh = sys.stdout

    try:
        writer = csv.writer(fh)

        for i, qr in enumerate(results):
            # Comment line identifying the check.
            fh.write(f"# {qr.category}/{qr.name}\n")

            # Header row.
            writer.writerow(qr.columns)

            # Data rows.
            for row in qr.rows:
                writer.writerow(row)

            # Blank line separator between checks.
            fh.write("\n")
    finally:
        if output_path and fh is not sys.stdout:
            fh.close()
