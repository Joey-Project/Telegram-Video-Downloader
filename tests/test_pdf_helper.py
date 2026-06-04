from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from scripts.pdf_helper import (
    archive_print_css,
    ensure_base_tag,
    extract_title_from_html,
    inject_head_style,
    is_bilibili_opus_url,
    is_placeholder_image_source,
    is_wechat_article_url,
    prepare_snapshot_html,
    print_page_pdf,
    reserve_unique_pdf_path,
    rewrite_lazy_image_sources,
    safe_stem,
    should_render_snapshot,
    strip_scripts,
)


class FailingPdfPage:
    async def pdf(self, path: str, **_: object) -> None:
        Path(path).write_text("partial")
        raise RuntimeError("print failed")


class PdfHelperTests(unittest.TestCase):
    def test_safe_stem_uses_title(self) -> None:
        self.assertEqual(safe_stem(" A / B: C? ", "https://example.com"), "A - B- C")

    def test_safe_stem_falls_back_to_host(self) -> None:
        self.assertEqual(safe_stem("", "https://example.com/article"), "example.com")

    def test_detects_wechat_article_url(self) -> None:
        self.assertTrue(is_wechat_article_url("https://mp.weixin.qq.com/s?__biz=abc"))
        self.assertFalse(is_wechat_article_url("https://example.com/s?__biz=abc"))

    def test_detects_bilibili_opus_url(self) -> None:
        self.assertTrue(is_bilibili_opus_url("https://www.bilibili.com/opus/1206098216310800386"))
        self.assertTrue(is_bilibili_opus_url("https://m.bilibili.com/opus/1206098216310800386"))
        self.assertFalse(is_bilibili_opus_url("https://www.bilibili.com/video/BV123"))
        self.assertFalse(is_bilibili_opus_url("https://www.bilibili.com/opus/not-a-number"))

    def test_snapshot_rendering_domains(self) -> None:
        self.assertTrue(should_render_snapshot("https://mp.weixin.qq.com/s?__biz=abc"))
        self.assertTrue(should_render_snapshot("https://www.bilibili.com/opus/1206098216310800386"))
        self.assertFalse(should_render_snapshot("https://example.com/article"))

    def test_ensure_base_tag_inserts_into_head(self) -> None:
        self.assertEqual(
            ensure_base_tag(
                "<html><head><title>x</title></head></html>", "https://mp.weixin.qq.com/s?x=1"
            ),
            '<html><head><base href="https://mp.weixin.qq.com/"><title>x</title></head></html>',
        )

    def test_ensure_base_tag_preserves_existing_base(self) -> None:
        html = '<html><head><base href="https://cdn.example/"></head></html>'
        self.assertEqual(ensure_base_tag(html, "https://mp.weixin.qq.com/s?x=1"), html)

    def test_inject_head_style_inserts_before_head_close(self) -> None:
        self.assertEqual(
            inject_head_style("<html><head><title>x</title></head></html>", "body{color:red}"),
            "<html><head><title>x</title><style>body{color:red}</style></head></html>",
        )

    def test_inject_head_style_handles_missing_head(self) -> None:
        self.assertEqual(
            inject_head_style("<main>Article</main>", "body{color:red}"),
            "<style>body{color:red}</style><main>Article</main>",
        )

    def test_bilibili_opus_archive_print_css_hides_page_chrome(self) -> None:
        css = archive_print_css("https://www.bilibili.com/opus/1206098216310800386")
        self.assertIn("#bili-header-container", css)
        self.assertIn(".opus-toc", css)
        self.assertIn(".opus-module-bottom__share", css)
        self.assertIn("max-width: 820px", css)
        self.assertEqual(archive_print_css("https://mp.weixin.qq.com/s?__biz=abc"), "")

    def test_prepare_snapshot_html_injects_bilibili_archive_css(self) -> None:
        html = (
            "<html><head><title>x</title></head><body>"
            "<script>window.close()</script>"
            '<img src="data:image/gif;base64,R0lGODlhAQABAAAAACw=" '
            'data-src="//i0.hdslb.com/article.jpg">'
            "</body></html>"
        )

        prepared = prepare_snapshot_html(html, "https://www.bilibili.com/opus/1206098216310800386")

        self.assertIn('<base href="https://www.bilibili.com/">', prepared)
        self.assertIn("#bili-header-container", prepared)
        self.assertIn('src="//i0.hdslb.com/article.jpg"', prepared)
        self.assertNotIn("window.close()", prepared)

    def test_prepare_snapshot_html_keeps_wechat_without_bilibili_css(self) -> None:
        prepared = prepare_snapshot_html(
            "<html><head></head><body>Article</body></html>",
            "https://mp.weixin.qq.com/s?__biz=abc",
        )

        self.assertIn('<base href="https://mp.weixin.qq.com/">', prepared)
        self.assertNotIn("#bili-header-container", prepared)

    def test_extract_title_from_wechat_html(self) -> None:
        html = '<h1 id="activity-name"> Example&nbsp;Title </h1><title>Fallback</title>'
        self.assertEqual(
            extract_title_from_html(html, "https://mp.weixin.qq.com/s?x=1"), "Example Title"
        )

    def test_strip_scripts_removes_inline_script(self) -> None:
        self.assertEqual(
            strip_scripts("<main>A</main><script>window.close()</script>"), "<main>A</main>"
        )

    def test_detects_placeholder_image_source(self) -> None:
        self.assertTrue(is_placeholder_image_source(""))
        self.assertTrue(is_placeholder_image_source("data:image/gif;base64,R0lGODlhAQABAAAAACw="))
        self.assertTrue(is_placeholder_image_source("/assets/transparent.gif"))
        self.assertFalse(is_placeholder_image_source("https://example.com/image.jpg"))

    def test_rewrite_lazy_image_sources_replaces_placeholder_src(self) -> None:
        html = (
            '<main><img src="data:image/gif;base64,R0lGODlhAQABAAAAACw=" '
            'data-src="https://res.wx.qq.com/article.jpg"></main>'
        )

        self.assertEqual(
            rewrite_lazy_image_sources(html),
            '<main><img src="https://res.wx.qq.com/article.jpg" '
            'data-src="https://res.wx.qq.com/article.jpg" loading="eager"></main>',
        )

    def test_rewrite_lazy_image_sources_keeps_real_src(self) -> None:
        html = (
            '<main><img src="https://example.com/already.jpg" '
            'data-src="https://res.wx.qq.com/article.jpg"></main>'
        )

        self.assertEqual(
            rewrite_lazy_image_sources(html),
            '<main><img src="https://example.com/already.jpg" '
            'data-src="https://res.wx.qq.com/article.jpg" loading="eager"></main>',
        )

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

    def test_print_page_pdf_removes_partial_file_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

            with self.assertRaisesRegex(RuntimeError, "print failed"):
                asyncio.run(
                    print_page_pdf(FailingPdfPage(), tmp_path, "Title", "https://example.com")
                )

            self.assertEqual(list(tmp_path.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
