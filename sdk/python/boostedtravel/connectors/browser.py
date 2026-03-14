"""
Shared browser launcher for BoostedTravel connectors.

Provides Chrome discovery, stealth CDP launch (off-screen, no-focus),
and Playwright helpers. All CDP Chrome connectors should use these
instead of rolling their own launch logic.

Environment variables:
    CHROME_PATH         — Override Chrome executable path.
    BOOSTED_BROWSER_VISIBLE — Set to "1" to show browser windows (debugging).
"""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import shutil
import subprocess
from typing import Optional

logger = logging.getLogger(__name__)

# ── Concurrency gate — limits how many browsers can run at once ──────────────
# Without this, 20+ Chrome processes spawn simultaneously and crash the machine.
_MAX_CONCURRENT_BROWSERS = 8
_browser_semaphore: Optional[asyncio.Semaphore] = None


async def _get_browser_semaphore() -> asyncio.Semaphore:
    """Get or create the global browser concurrency semaphore (lazy init)."""
    global _browser_semaphore
    if _browser_semaphore is None:
        _browser_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_BROWSERS)
    return _browser_semaphore


async def acquire_browser_slot():
    """Acquire a browser slot — blocks if too many browsers are running."""
    sem = await _get_browser_semaphore()
    await sem.acquire()


def release_browser_slot():
    """Release a browser slot after a connector finishes with its browser."""
    if _browser_semaphore is not None:
        _browser_semaphore.release()


# ── Cleanup registry — tracks resources launched by connectors ───────────────
_launched_procs: list[subprocess.Popen] = []
_launched_pw_instances: list = []

# ── Chrome discovery ────────────────────────────────────────────────────────────

_WIN_CANDIDATES = [
    os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
    os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
    os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
]

_LINUX_CANDIDATES = [
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/usr/bin/chromium-browser",
    "/usr/bin/chromium",
]

_MAC_CANDIDATES = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
]


def find_chrome() -> str:
    """Find Chrome executable on the system. Raises RuntimeError if not found."""
    env = os.environ.get("CHROME_PATH", "")
    if env and os.path.isfile(env):
        return env

    system = platform.system()
    if system == "Windows":
        candidates = _WIN_CANDIDATES
    elif system == "Darwin":
        candidates = _MAC_CANDIDATES
    else:
        candidates = _LINUX_CANDIDATES

    # Also try PATH
    which = shutil.which("google-chrome") or shutil.which("chrome") or shutil.which("chromium")
    if which:
        candidates = [which] + candidates

    for c in candidates:
        if c and os.path.isfile(c):
            return c

    raise RuntimeError(
        "Chrome not found. Install Google Chrome or set CHROME_PATH env var."
    )


# ── Stealth window args ────────────────────────────────────────────────────────

def _is_visible() -> bool:
    """Check if browsers should be visible (for debugging)."""
    return os.environ.get("BOOSTED_BROWSER_VISIBLE", "").strip() in ("1", "true", "yes")


def is_headless() -> bool:
    """Returns True when browsers should run headless (default).

    Set BOOSTED_BROWSER_VISIBLE=1 to force headed mode for debugging.
    """
    return not _is_visible()


def stealth_args() -> list[str]:
    """
    Chrome CLI args for invisible operation.

    Default: --headless=new (Chrome's modern undetectable headless).
    When BOOSTED_BROWSER_VISIBLE=1, returns empty list (normal headed window).
    """
    if _is_visible():
        return []
    return [
        "--headless=new",
        "--disable-http2",
        "--window-position=-2400,-2400",
        "--window-size=800,600",
    ]


def stealth_position_arg() -> list[str]:
    """Headless + off-screen position (for connectors that set their own --window-size)."""
    if _is_visible():
        return []
    return ["--headless=new", "--disable-http2", "--window-position=-2400,-2400"]


def stealth_popen_kwargs() -> dict:
    """
    Extra kwargs for subprocess.Popen to suppress the Chrome window on Windows.

    Uses STARTUPINFO with SW_SHOWMINNOACTIVE so the window starts minimized
    without stealing focus. On Linux/Mac, suppresses stdout/stderr.
    """
    kwargs: dict = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }

    if _is_visible():
        return kwargs

    if platform.system() == "Windows":
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = 7  # SW_SHOWMINNOACTIVE — minimized, no focus steal
        kwargs["startupinfo"] = si
        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
    return kwargs


# ── CDP Chrome launch ──────────────────────────────────────────────────────────

async def launch_cdp_chrome(
    port: int,
    user_data_dir: str,
    *,
    extra_args: Optional[list[str]] = None,
    start_url: str = "about:blank",
    startup_wait: float = 2.0,
) -> subprocess.Popen:
    """
    Launch a real Chrome instance for CDP connection.

    The window is pushed off-screen and minimized so it doesn't
    disturb the user. Set BOOSTED_BROWSER_VISIBLE=1 to override.

    Args:
        port: CDP debugging port (e.g. 9460).
        user_data_dir: Path to Chrome user data directory.
        extra_args: Additional Chrome CLI flags.
        start_url: Initial URL to load (default: about:blank).
        startup_wait: Seconds to wait after launch for Chrome to be ready.

    Returns:
        The subprocess.Popen handle.
    """
    chrome = find_chrome()
    os.makedirs(user_data_dir, exist_ok=True)

    args = [
        chrome,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-blink-features=AutomationControlled",
        *stealth_args(),
        *(extra_args or []),
        start_url,
    ]

    proc = subprocess.Popen(args, **stealth_popen_kwargs())
    _launched_procs.append(proc)
    await asyncio.sleep(startup_wait)
    logger.info("Chrome launched on CDP port %d (pid %d, visible=%s)",
                port, proc.pid, _is_visible())
    return proc


async def connect_cdp(port: int):
    """
    Connect Playwright to an existing Chrome via CDP.

    Returns the Playwright Browser object.
    """
    from playwright.async_api import async_playwright
    pw = await async_playwright().start()
    _launched_pw_instances.append(pw)
    browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
    return browser


async def get_or_launch_cdp(
    port: int,
    user_data_dir: str,
    *,
    extra_args: Optional[list[str]] = None,
    start_url: str = "about:blank",
    startup_wait: float = 2.0,
):
    """
    Try connecting to existing Chrome on port, or launch a new one.

    Returns (browser, proc_or_None).  Playwright instances are tracked
    in ``_launched_pw_instances`` so ``cleanup_all_browsers()`` can stop
    them later.
    """
    from playwright.async_api import async_playwright

    # Try connecting to already-running Chrome
    pw = None
    try:
        pw = await async_playwright().start()
        browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
        _launched_pw_instances.append(pw)
        logger.info("Connected to existing Chrome on port %d", port)
        return browser, None
    except Exception:
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass

    # Launch new Chrome
    proc = await launch_cdp_chrome(
        port, user_data_dir,
        extra_args=extra_args,
        start_url=start_url,
        startup_wait=startup_wait,
    )
    pw = await async_playwright().start()
    _launched_pw_instances.append(pw)
    browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
    return browser, proc


# ── Playwright headed launch ───────────────────────────────────────────────────

async def launch_headed_browser(
    *,
    channel: str = "chrome",
    extra_args: Optional[list[str]] = None,
):
    """
    Launch a headed Playwright browser with stealth window positioning.

    Used by connectors that need Playwright.launch(headless=False) instead of CDP.
    Window is pushed off-screen unless BOOSTED_BROWSER_VISIBLE=1.

    Returns the Playwright Browser object.
    """
    from playwright.async_api import async_playwright

    args = [*stealth_args(), *(extra_args or [])]

    headless = is_headless()
    pw = await async_playwright().start()
    _launched_pw_instances.append(pw)
    try:
        browser = await pw.chromium.launch(
            headless=headless,
            channel=channel,
            args=args if args else None,
        )
    except Exception:
        # Fallback: no channel (use bundled Chromium)
        browser = await pw.chromium.launch(
            headless=headless,
            args=args if args else None,
        )
    logger.info("Browser launched (channel=%s, headless=%s)", channel, headless)
    return browser


# ── Cleanup ─────────────────────────────────────────────────────────────────────

async def cleanup_module_browsers(*modules) -> int:
    """
    Clean up browser resources stored in module-level globals.

    Introspects each module for known global names (_browser, _chrome_proc,
    _pw_instance, _nd_browser, _cdp_browser, _context, _warm_page) and
    closes/terminates them.  Returns number of resources closed.
    """
    closed = 0
    for mod in modules:
        # Close Playwright browser context (easyjet)
        ctx = getattr(mod, '_context', None)
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass
            mod._context = None
            closed += 1

        # Close warm page (flynas keepalive)
        wp = getattr(mod, '_warm_page', None)
        if wp:
            try:
                await wp.close()
            except Exception:
                pass
            mod._warm_page = None
            closed += 1

        # Close primary Playwright browser
        for attr in ('_browser', '_cdp_browser'):
            browser = getattr(mod, attr, None)
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass
                setattr(mod, attr, None)
                closed += 1

        # Quit nodriver browser (twayair)
        nd = getattr(mod, '_nd_browser', None)
        if nd:
            try:
                nd.stop()
            except Exception:
                pass
            mod._nd_browser = None
            closed += 1

        # Stop Playwright instances
        pw = getattr(mod, '_pw_instance', None)
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass
            mod._pw_instance = None
            closed += 1

        # Terminate Chrome subprocess
        proc = getattr(mod, '_chrome_proc', None)
        if proc:
            try:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            mod._chrome_proc = None
            closed += 1

    return closed


async def cleanup_all_browsers():
    """
    Terminate all Chrome processes and Playwright instances launched via
    browser.py helper functions (launch_cdp_chrome, launch_headed_browser).

    Only affects processes this module created — never touches the user's
    own Chrome windows.
    """
    closed = 0

    # Stop Playwright instances launched by launch_headed_browser / connect_cdp
    for pw in _launched_pw_instances:
        try:
            await pw.stop()
            closed += 1
        except Exception:
            pass
    _launched_pw_instances.clear()

    # Terminate Chrome subprocesses launched by launch_cdp_chrome
    for proc in _launched_procs:
        try:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5)
                closed += 1
        except Exception:
            try:
                proc.kill()
                closed += 1
            except Exception:
                pass
    _launched_procs.clear()

    # Reset the semaphore so it's fresh for next search
    global _browser_semaphore
    _browser_semaphore = None

    if closed:
        logger.info("browser.py cleanup: terminated %d browser resources", closed)

    return closed
