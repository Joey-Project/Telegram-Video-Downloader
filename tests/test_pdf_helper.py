from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from scripts.pdf_helper import reserve_unique_pdf_path, safe_stem


class PdfHelperTests(unittest.TestCase):
    def test_safe_stem_uses_title(self) -> None:
        self.assertEqual(safe_stem(" A / B: C? ", "https://example.com"), "A - B- C")

    def test_safe_stem_falls_back_to_host(self) -> None:
        self.assertEqual(safe_stem("", "https://example.com/article"), "example.com")

    def test_unique_pdf_path_adds_collision_suffix(self) -> None:
        now = datetime(2026, 5, 16, 12, 30, 0)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            first = tmp_path / "20260516-123000-Title.pdf"
            first.write_text("exists")

            self.assertEqual(
                reserve_unique_pdf_path(tmp_path, "Title", "https://example.com", now).path,
                tmp_path / "20260516-123000-Title-2.pdf",
            )

    def test_reserved_pdf_path_skips_existing_lock(self) -> None:
        now = datetime(2026, 5, 16, 12, 30, 0)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            lock = tmp_path / "20260516-123000-Title.pdf.lock"
            lock.write_text("locked")

            reservation = reserve_unique_pdf_path(tmp_path, "Title", "https://example.com", now)
            try:
                self.assertEqual(reservation.path, tmp_path / "20260516-123000-Title-2.pdf")
            finally:
                reservation.release()


if __name__ == "__main__":
    unittest.main()
