from __future__ import annotations

import argparse
import asyncio
import re
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse


def safe_stem(title: str, url: str, max_length: int = 96) -> str:
    source = title.strip() or urlparse(url).netloc or "page"
    cleaned = re.sub(r"[^\w .-]+", "-", source, flags=re.UNICODE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .-_")
    cleaned = cleaned[:max_length].strip(" .-_")
    return cleaned or "page"


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


async def render_pdf(url: str, output_dir: Path, chrome: Path) -> Path:
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
    from playwright.async_api import async_playwright

    output_dir.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            executable_path=str(chrome),
            headless=True,
            args=["--disable-gpu"],
        )
        try:
            page = await browser.new_page(viewport={"width": 1440, "height": 1000})
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            with suppress(PlaywrightTimeoutError):
                await page.wait_for_load_state("networkidle", timeout=20_000)

            await scroll_until_stable(page)
            with suppress(PlaywrightTimeoutError):
                await page.wait_for_load_state("networkidle", timeout=10_000)

            title = await page.title()
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
            finally:
                reservation.release()
        finally:
            await browser.close()


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
