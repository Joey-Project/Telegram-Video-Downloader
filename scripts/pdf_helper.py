from __future__ import annotations

import argparse
import asyncio
import html as html_lib
import re
import urllib.request
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse

DESKTOP_CHROME_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

LAZY_IMAGE_SOURCE_ATTRIBUTES = (
    "data-src",
    "data-original",
    "data-lazy-src",
    "data-backsrc",
    "data-origin-src",
    "data-croporisrc",
)
PLACEHOLDER_IMAGE_SOURCE_TOKENS = (
    "placeholder",
    "transparent",
    "spacer",
    "blank",
    "loading",
    "pixel",
)


def safe_stem(title: str, url: str, max_length: int = 96) -> str:
    source = title.strip() or urlparse(url).netloc or "page"
    cleaned = re.sub(r"[^\w .-]+", "-", source, flags=re.UNICODE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .-_")
    cleaned = cleaned[:max_length].strip(" .-_")
    return cleaned or "page"


def is_wechat_article_url(url: str) -> bool:
    return urlparse(url).netloc.lower() == "mp.weixin.qq.com"


def is_bilibili_opus_url(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host != "bilibili.com" and not host.endswith(".bilibili.com"):
        return False

    segments = [segment for segment in parsed.path.split("/") if segment]
    return len(segments) >= 2 and segments[0] == "opus" and segments[1].isdigit()


def should_render_snapshot(url: str) -> bool:
    return is_wechat_article_url(url) or is_bilibili_opus_url(url)


def ensure_base_tag(html: str, url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return html

    base_tag = f'<base href="{parsed.scheme}://{parsed.netloc}/">'
    if re.search(r"<base\b", html, flags=re.IGNORECASE):
        return html

    head_match = re.search(r"<head\b[^>]*>", html, flags=re.IGNORECASE)
    if head_match:
        insert_at = head_match.end()
        return f"{html[:insert_at]}{base_tag}{html[insert_at:]}"

    return f"{base_tag}{html}"


def inject_head_style(html: str, css: str) -> str:
    if not css.strip():
        return html

    style_tag = f"<style>{css}</style>"
    head_end = re.search(r"</head\s*>", html, flags=re.IGNORECASE)
    if head_end:
        insert_at = head_end.start()
        return f"{html[:insert_at]}{style_tag}{html[insert_at:]}"

    head_start = re.search(r"<head\b[^>]*>", html, flags=re.IGNORECASE)
    if head_start:
        insert_at = head_start.end()
        return f"{html[:insert_at]}{style_tag}{html[insert_at:]}"

    return f"{style_tag}{html}"


def archive_print_css(url: str) -> str:
    if not is_bilibili_opus_url(url):
        return ""

    return """
@media screen, print {
  html,
  body {
    background: #fff !important;
  }

  body {
    margin: 0 !important;
  }

  #bili-header-container,
  .bg,
  .bgc,
  .opus-toc,
  .opus-more,
  .opus-module-bottom__feedback,
  .opus-module-bottom__share,
  .opus-module-author__more {
    display: none !important;
  }

  #app,
  .opus-detail,
  .bili-opus-view-wrap {
    width: 100% !important;
    max-width: none !important;
    margin: 0 !important;
    background: #fff !important;
    box-shadow: none !important;
  }

  .bili-opus-view {
    max-width: 820px !important;
    margin: 0 auto !important;
    padding: 0 !important;
    background: #fff !important;
  }

  .opus-module-title__text {
    color: #18191c !important;
    font-size: 28px !important;
    line-height: 1.35 !important;
  }

  .opus-module-content {
    color: #18191c !important;
    font-size: 16px !important;
    line-height: 1.75 !important;
  }

  .opus-para-pic,
  .bili-dyn-pic,
  .opus-pic-view {
    break-inside: avoid !important;
    page-break-inside: avoid !important;
  }

  img,
  video {
    max-width: 100% !important;
    height: auto !important;
  }
}
"""


def prepare_snapshot_html(html: str, url: str) -> str:
    html = strip_scripts(html)
    html = ensure_base_tag(html, url)
    html = rewrite_lazy_image_sources(html)
    return inject_head_style(html, archive_print_css(url))


def strip_scripts(html: str) -> str:
    return re.sub(r"(?is)<script\b[^>]*>.*?</script>", "", html)


def clean_text(value: str) -> str:
    value = re.sub(r"(?is)<[^>]+>", "", value)
    value = html_lib.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def extract_title_from_html(html: str, url: str) -> str:
    patterns = [
        r'(?is)<h1\b[^>]*id=["\']activity-name["\'][^>]*>(.*?)</h1>',
        r'(?is)<h1\b[^>]*class=["\'][^"\']*\brich_media_title\b[^"\']*["\'][^>]*>(.*?)</h1>',
        r'(?is)<meta\b[^>]*(?:property|name)=["\'](?:og:title|twitter:title)["\'][^>]*content=["\']([^"\']+)["\']',
        r"(?is)<title\b[^>]*>(.*?)</title>",
    ]

    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            title = clean_text(match.group(1))
            if title:
                return title

    return urlparse(url).netloc or "page"


def fetch_page_html(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": DESKTOP_CHROME_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def is_placeholder_image_source(src: str | None) -> bool:
    if not src:
        return True

    normalized = src.strip().lower()
    return (
        not normalized
        or normalized in {"#", "about:blank"}
        or normalized.startswith("data:")
        or any(token in normalized for token in PLACEHOLDER_IMAGE_SOURCE_TOKENS)
    )


def rewrite_image_attrs(attrs: list[tuple[str, str | None]]) -> list[tuple[str, str | None]]:
    lower_values = {name.lower(): value for name, value in attrs}
    lazy_src = next(
        (lower_values[attr] for attr in LAZY_IMAGE_SOURCE_ATTRIBUTES if lower_values.get(attr)),
        None,
    )
    lazy_srcset = lower_values.get("data-srcset")
    current_src = lower_values.get("src")
    current_srcset = lower_values.get("srcset")

    rewritten = []
    saw_src = False
    saw_srcset = False
    saw_loading = False
    for name, value in attrs:
        lower_name = name.lower()
        if lower_name == "src":
            saw_src = True
            if lazy_src and is_placeholder_image_source(value):
                value = lazy_src
        elif lower_name == "srcset":
            saw_srcset = True
            if lazy_srcset and not current_srcset:
                value = lazy_srcset
        elif lower_name == "loading":
            saw_loading = True
            value = "eager"

        rewritten.append((name, value))

    if lazy_src and not saw_src and is_placeholder_image_source(current_src):
        rewritten.append(("src", lazy_src))
    if lazy_srcset and not saw_srcset:
        rewritten.append(("srcset", lazy_srcset))
    if not saw_loading:
        rewritten.append(("loading", "eager"))

    return rewritten


def render_html_start_tag(tag: str, attrs: list[tuple[str, str | None]], self_closing: bool) -> str:
    rendered_attrs = []
    for name, value in attrs:
        if value is None:
            rendered_attrs.append(name)
        else:
            rendered_attrs.append(f'{name}="{html_lib.escape(value, quote=True)}"')

    attr_text = "" if not rendered_attrs else " " + " ".join(rendered_attrs)
    suffix = " /" if self_closing else ""
    return f"<{tag}{attr_text}{suffix}>"


class LazyImageHtmlRewriter(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "img":
            attrs = rewrite_image_attrs(attrs)
        self.parts.append(render_html_start_tag(tag, attrs, self_closing=False))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "img":
            attrs = rewrite_image_attrs(attrs)
        self.parts.append(render_html_start_tag(tag, attrs, self_closing=True))

    def handle_endtag(self, tag: str) -> None:
        self.parts.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def handle_entityref(self, name: str) -> None:
        self.parts.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        self.parts.append(f"&#{name};")

    def handle_comment(self, data: str) -> None:
        self.parts.append(f"<!--{data}-->")

    def handle_decl(self, decl: str) -> None:
        self.parts.append(f"<!{decl}>")

    def handle_pi(self, data: str) -> None:
        self.parts.append(f"<?{data}>")

    def rewritten_html(self) -> str:
        return "".join(self.parts)


def rewrite_lazy_image_sources(html: str) -> str:
    rewriter = LazyImageHtmlRewriter()
    rewriter.feed(html)
    rewriter.close()
    return rewriter.rewritten_html()


@dataclass(frozen=True)
class ReservedPdfPath:
    path: Path
    lock_path: Path

    def release(self) -> None:
        self.lock_path.unlink(missing_ok=True)


def reserve_unique_pdf_path(
    output_dir: Path, title: str, url: str, now: datetime | None = None
) -> ReservedPdfPath:
    timestamp = (now or datetime.now()).strftime("%Y%m%d-%H%M%S")
    stem = safe_stem(title, url)

    for index in range(1, 1000):
        suffix = "" if index == 1 else f"-{index}"
        candidate = output_dir / f"{timestamp}-{stem}{suffix}.pdf"
        lock_path = candidate.with_suffix(f"{candidate.suffix}.lock")
        if candidate.exists():
            continue

        try:
            with lock_path.open("x"):
                pass
        except FileExistsError:
            continue

        if candidate.exists():
            lock_path.unlink(missing_ok=True)
            continue

        return ReservedPdfPath(path=candidate, lock_path=lock_path)

    raise RuntimeError("could not allocate unique PDF path")


async def scroll_until_stable(page, max_rounds: int = 80, stable_rounds: int = 3) -> None:
    stable_count = 0
    previous_height = -1

    for _ in range(max_rounds):
        height = await page.evaluate(
            "() => Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)"
        )
        viewport_height = await page.evaluate("() => window.innerHeight")
        await page.evaluate(
            "(amount) => window.scrollBy(0, amount)", max(200, viewport_height - 100)
        )
        await page.wait_for_timeout(500)

        new_height = await page.evaluate(
            "() => Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)"
        )
        at_bottom = await page.evaluate(
            "() => window.scrollY + window.innerHeight >= "
            "Math.max(document.body.scrollHeight, document.documentElement.scrollHeight) - 4"
        )

        if at_bottom and new_height == previous_height:
            stable_count += 1
        else:
            stable_count = 0

        if stable_count >= stable_rounds:
            break

        previous_height = new_height if new_height != height else height

    await page.evaluate("() => window.scrollTo(0, 0)")
    await page.wait_for_timeout(500)


async def print_page_pdf(page, output_dir: Path, title: str, url: str) -> Path:
    reservation = reserve_unique_pdf_path(output_dir, title, url)
    try:
        await page.pdf(
            path=str(reservation.path),
            format="A4",
            print_background=True,
            margin={"top": "12mm", "right": "12mm", "bottom": "12mm", "left": "12mm"},
            prefer_css_page_size=True,
        )
        return reservation.path
    except Exception:
        reservation.path.unlink(missing_ok=True)
        raise
    finally:
        reservation.release()


async def prepare_snapshot_dom(page) -> str:
    await page.evaluate(
        """
        () => {
          const imageAttrs = [
            'data-src',
            'data-original',
            'data-lazy-src',
            'data-backsrc',
            'data-origin-src',
            'data-croporisrc',
          ];

          for (const img of document.querySelectorAll('img')) {
            const src = imageAttrs.map((attr) => img.getAttribute(attr)).find(Boolean);
            const currentSrc = (img.getAttribute('src') || '').trim().toLowerCase();
            const isPlaceholder = !currentSrc ||
              currentSrc === '#' ||
              currentSrc === 'about:blank' ||
              currentSrc.startsWith('data:') ||
              ['placeholder', 'transparent', 'spacer', 'blank', 'loading', 'pixel']
                .some((token) => currentSrc.includes(token));

            if (src && isPlaceholder) {
              img.setAttribute('src', src);
            }

            const srcset = img.getAttribute('data-srcset');
            if (srcset && !img.getAttribute('srcset')) {
              img.setAttribute('srcset', srcset);
            }

            img.setAttribute('loading', 'eager');
            img.style.visibility = 'visible';
            img.style.opacity = '1';
          }

          if (document.documentElement) {
            document.documentElement.style.visibility = 'visible';
          }
          if (document.body) {
            document.body.style.display = 'block';
            document.body.style.visibility = 'visible';
            document.body.style.opacity = '1';
          }

          for (const source of document.querySelectorAll('source')) {
            const srcset = source.getAttribute('data-srcset');
            if (srcset && !source.getAttribute('srcset')) {
              source.setAttribute('srcset', srcset);
            }
          }

          for (const script of document.querySelectorAll('script')) {
            script.remove();
          }
        }
        """
    )
    return await page.content()


async def wait_for_images(page, timeout_ms: int = 15_000) -> None:
    await page.evaluate(
        """
        async (timeoutMs) => {
          const waitForImage = (img) => new Promise((resolve) => {
            if (img.complete) {
              resolve();
              return;
            }

            const timer = setTimeout(resolve, timeoutMs);
            img.onload = img.onerror = () => {
              clearTimeout(timer);
              resolve();
            };
          });

          await Promise.all(Array.from(document.images).map(waitForImage));
        }
        """,
        timeout_ms,
    )


async def render_snapshot_pdf(browser, url: str, output_dir: Path) -> Path:
    from playwright.async_api import Error as PlaywrightError

    html = fetch_page_html(url)
    title = extract_title_from_html(html, url)
    html = prepare_snapshot_html(html, url)

    snapshot_context = await browser.new_context(
        user_agent=DESKTOP_CHROME_USER_AGENT,
        extra_http_headers={"Referer": url},
        viewport={"width": 1440, "height": 1000},
    )
    try:
        snapshot_page = await snapshot_context.new_page()
        await snapshot_page.set_content(html, wait_until="domcontentloaded", timeout=60_000)
        await prepare_snapshot_dom(snapshot_page)
        await wait_for_images(snapshot_page)
        await scroll_until_stable(snapshot_page, max_rounds=12, stable_rounds=2)
        return await print_page_pdf(snapshot_page, output_dir, title, url)
    finally:
        with suppress(PlaywrightError):
            await snapshot_context.close()


async def render_pdf(url: str, output_dir: Path, chrome: Path) -> Path:
    from playwright.async_api import Error as PlaywrightError
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
    from playwright.async_api import async_playwright

    output_dir.mkdir(parents=True, exist_ok=True)

    async def render_with_browser(executable_path: Path | None) -> Path:
        launch_options = {
            "headless": True,
            "args": ["--disable-gpu"],
        }
        if executable_path is not None:
            launch_options["executable_path"] = str(executable_path)

        browser = await playwright.chromium.launch(**launch_options)
        try:
            if should_render_snapshot(url):
                return await render_snapshot_pdf(browser, url, output_dir)

            page = await browser.new_page(viewport={"width": 1440, "height": 1000})
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            with suppress(PlaywrightTimeoutError):
                await page.wait_for_load_state("networkidle", timeout=20_000)

            await scroll_until_stable(page)
            with suppress(PlaywrightTimeoutError):
                await page.wait_for_load_state("networkidle", timeout=10_000)

            title = await page.title()
            return await print_page_pdf(page, output_dir, title, url)
        finally:
            with suppress(PlaywrightError):
                await browser.close()

    async with async_playwright() as playwright:
        try:
            return await render_with_browser(chrome)
        except PlaywrightError:
            if not should_render_snapshot(url):
                raise

            return await render_with_browser(None)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render a lazy-loaded web page to PDF.")
    parser.add_argument("--url", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--chrome", required=True, type=Path)
    return parser.parse_args()


async def async_main() -> int:
    args = parse_args()
    output_path = await render_pdf(args.url, args.output_dir, args.chrome)
    print(output_path)
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
