"""Unified Claude caller — CLI-first, API fallback.

PRIMARY MODE: Claude CLI subprocess
  - Uses `claude --print` (non-interactive output mode)
  - Works with your Claude Code subscription — no API key needed
  - Installed via: npm install -g @anthropic-ai/claude-code
  - Auth via: claude auth login  (run once on VPS)
  - Or set:  CLAUDE_CODE_OAUTH_TOKEN env var for headless auth

FALLBACK MODE: Anthropic Python SDK
  - Only used if `claude` binary is not on PATH
  - Requires ANTHROPIC_API_KEY in config/.env

VPS setup (Ubuntu/Debian):
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
    sudo apt-get install -y nodejs
    npm install -g @anthropic-ai/claude-code
    claude auth login        # interactive, run once
    # OR headless:
    export CLAUDE_CODE_OAUTH_TOKEN=your_token_here
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess

from config.settings import ANTHROPIC_API_KEY, MODEL

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CLI detection (Linux/VPS first, Windows fallback)
# ---------------------------------------------------------------------------

def _find_claude_cli() -> str | None:
    """Find the claude binary — checks PATH then common install locations."""

    # 1. Standard PATH lookup (works on any OS after `npm install -g`)
    found = shutil.which("claude")
    if found:
        return found

    home = os.path.expanduser("~")

    # 2. Linux/Mac locations
    linux_candidates = [
        # npm global (most common on Linux VPS)
        os.path.join(home, ".npm-global", "bin", "claude"),
        os.path.join(home, ".local", "share", "npm", "bin", "claude"),
        "/usr/local/bin/claude",
        "/usr/bin/claude",
        # nvm installs
        os.path.join(home, ".nvm", "versions", "node"),   # parent — walk below
    ]
    for path in linux_candidates:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
        # Walk nvm node versions
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                if "bin" in root and "claude" in files:
                    return os.path.join(root, "claude")

    # 3. Windows locations (fallback for local dev)
    win_candidates = [
        os.path.join(home, "AppData", "Roaming", "npm", "claude.cmd"),
        os.path.join(home, "AppData", "Roaming", "npm", "claude"),
    ]
    for path in win_candidates:
        if os.path.isfile(path):
            return path

    # 4. Check if running inside a npx cache (Windows dev)
    npx_cache = os.path.join(home, "AppData", "Local", "npm-cache", "_npx")
    if os.path.isdir(npx_cache):
        for root, dirs, files in os.walk(npx_cache):
            for f in files:
                if f in ("claude.cmd", "claude.exe", "claude"):
                    return os.path.join(root, f)

    return None


_CLI_PATH: str | None = _find_claude_cli()
USE_CLI: bool = _CLI_PATH is not None

if USE_CLI:
    log.debug("claude.mode=cli  path=%s", _CLI_PATH)
else:
    log.debug("claude.mode=api  key_set=%s", bool(ANTHROPIC_API_KEY))


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def call_claude(
    prompt: str,
    system: str = "",
    max_tokens: int = 4096,
    model: str | None = None,
    force_api: bool = False,
) -> str:
    """Call Claude and return the raw text response.

    Args:
        prompt:     User message.
        system:     System prompt (optional — prepended when using CLI).
        max_tokens: Max tokens (applies to API mode; CLI uses its own defaults).
        model:      Model override. For CLI: "sonnet", "opus", "haiku".
                    For API: full model ID.
        force_api:  Skip CLI and use API directly (for testing).

    Returns:
        Raw text string from Claude.

    Raises:
        RuntimeError: If both CLI and API fail.
    """
    if USE_CLI and not force_api:
        return _call_cli(prompt, system, model)
    return _call_api(prompt, system, max_tokens, model)


def call_claude_json(
    prompt: str,
    system: str = "",
    max_tokens: int = 4096,
    model: str | None = None,
) -> dict | list:
    """Call Claude and return a parsed JSON object/array.

    Strips markdown fences and extracts the first valid JSON token.

    Returns:
        Parsed dict or list. Returns {} on parse failure.
    """
    raw = call_claude(prompt, system, max_tokens, model)

    # Strip markdown code fences
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    # Find first JSON token
    start = next((i for i, ch in enumerate(raw) if ch in "{["), -1)
    if start > 0:
        raw = raw[start:]

    try:
        data, _ = json.JSONDecoder().raw_decode(raw)
        return data
    except json.JSONDecodeError:
        log.error("claude.json_parse_fail  raw_preview=%s", raw[:300])
        return {}


def call_claude_raw(
    model: str | None = None,
    max_tokens: int = 4096,
    messages: list[dict] | None = None,
    system: str = "",
    **_kwargs,
):
    """Compatibility wrapper — mirrors the anthropic SDK response shape.

    Used by handlers that previously called self.client.messages.create().
    Returns an object with .content[0].text just like the anthropic SDK.
    """
    prompt_parts = [
        msg.get("content", "")
        for msg in (messages or [])
        if msg.get("role") == "user"
    ]
    prompt = "\n\n".join(prompt_parts)
    raw = call_claude(prompt, system=system, max_tokens=max_tokens, model=model)

    class _TextBlock:
        def __init__(self, text: str):
            self.text = text

    class _Response:
        def __init__(self, text: str):
            self.content = [_TextBlock(text)]

    return _Response(raw)


def get_mode() -> str:
    """Return 'cli' or 'api'."""
    return "cli" if (USE_CLI and _CLI_PATH) else "api"


def verify_cli() -> dict:
    """Check that the Claude CLI is installed, authenticated, and working.

    Returns:
        dict with keys: available, path, version, authenticated, mode, error
    """
    result = {
        "available":     USE_CLI,
        "path":          _CLI_PATH or "",
        "version":       "",
        "authenticated": False,
        "mode":          get_mode(),
        "error":         "",
    }

    if not USE_CLI:
        result["error"] = (
            "Claude CLI not found. Install with: npm install -g @anthropic-ai/claude-code"
        )
        return result

    # Check version
    try:
        ver = subprocess.run(
            [_CLI_PATH, "--version"],
            capture_output=True, text=True, timeout=10,
            shell=_CLI_PATH.endswith(".cmd"),
        )
        result["version"] = ver.stdout.strip() or ver.stderr.strip()
    except Exception as e:
        result["error"] = f"version check failed: {e}"

    # Quick auth smoke test — send a trivial prompt
    try:
        test = subprocess.run(
            _build_cmd("Reply with the single word: OK", "", "haiku"),
            capture_output=True, text=True, timeout=30,
            shell=_CLI_PATH.endswith(".cmd"),
            encoding="utf-8", errors="replace",
        )
        if test.returncode == 0 and "OK" in test.stdout:
            result["authenticated"] = True
        else:
            result["error"] = f"auth test failed: {test.stderr[:200]}"
    except Exception as e:
        result["error"] = f"auth smoke test error: {e}"

    return result


# ---------------------------------------------------------------------------
# Internal: CLI call
# ---------------------------------------------------------------------------

def _call_cli(prompt: str, system: str, model: str | None) -> str:
    """Run Claude CLI as a subprocess and return stdout."""
    cmd = _build_cmd(prompt, system, model)
    use_shell = bool(_CLI_PATH) and _CLI_PATH.endswith(".cmd")   # Windows .cmd needs shell=True

    log.info(
        "claude.cli  model=%s  prompt_chars=%d  path=%s",
        model or "sonnet", len(prompt), _CLI_PATH,
    )

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
            shell=use_shell,
            encoding="utf-8",
            errors="replace",
        )

        if proc.returncode != 0:
            stderr = (proc.stderr or "")[:500]
            log.error("claude.cli_fail  code=%d  stderr=%s", proc.returncode, stderr)

            # If auth error — give clear guidance
            if "auth" in stderr.lower() or "login" in stderr.lower() or "401" in stderr.lower():
                raise RuntimeError(
                    "Claude CLI authentication failed. Run: claude auth login\n"
                    "Or set CLAUDE_CODE_OAUTH_TOKEN environment variable."
                )
            raise RuntimeError(f"Claude CLI returned code {proc.returncode}: {stderr}")

        output = proc.stdout.strip()
        if not output:
            raise RuntimeError("Claude CLI returned empty response")

        log.info("claude.cli_done  chars=%d", len(output))
        return output

    except subprocess.TimeoutExpired:
        log.error("claude.cli_timeout  prompt_chars=%d", len(prompt))
        raise RuntimeError(
            "Claude CLI timed out after 300s. "
            "Prompt may be too large or the CLI is busy. "
            "Try reducing max_tokens or splitting the request."
        )
    except FileNotFoundError:
        log.warning("claude.cli_not_found  path=%s  falling_back_to_api", _CLI_PATH)
        return _call_api(prompt, "", 4096, model)


def _build_cmd(prompt: str, system: str, model: str | None) -> list[str]:
    """Build the claude CLI command list."""
    # Normalise model name — CLI accepts "sonnet", "opus", "haiku" or full IDs
    model_arg = _normalise_model(model)

    cmd = [
        _CLI_PATH,
        "--print",                   # non-interactive, print response and exit
        "--model",   model_arg,
        "--max-turns", "1",          # single turn for pipeline tasks
    ]

    if system:
        cmd.extend(["--system-prompt", system])

    # Prompt is passed via stdin (avoids shell-quoting issues with long prompts)
    cmd.extend(["--message", prompt])

    return cmd


def _normalise_model(model: str | None) -> str:
    """Normalise model name for the CLI.

    CLI accepts short names: sonnet, opus, haiku
    or full IDs: claude-sonnet-4-5, claude-opus-4-5, etc.
    """
    if not model:
        return "sonnet"

    model_lower = model.lower()
    if "opus" in model_lower:
        return "opus"
    if "haiku" in model_lower:
        return "haiku"
    if "sonnet" in model_lower:
        return "sonnet"
    # Return as-is if it looks like a full model ID
    return model


# ---------------------------------------------------------------------------
# Internal: API call (fallback)
# ---------------------------------------------------------------------------

def _call_api(prompt: str, system: str, max_tokens: int, model: str | None) -> str:
    """Call Claude via the Anthropic Python SDK (fallback when CLI unavailable)."""
    try:
        import anthropic
    except ImportError:
        raise RuntimeError(
            "Claude CLI not found and the 'anthropic' package is not installed.\n"
            "Install CLI:  npm install -g @anthropic-ai/claude-code\n"
            "Or install:   pip install anthropic  and set ANTHROPIC_API_KEY"
        )

    if not ANTHROPIC_API_KEY:
        raise RuntimeError(
            "Claude CLI not found and ANTHROPIC_API_KEY is not set.\n"
            "Option 1: Install Claude CLI  →  npm install -g @anthropic-ai/claude-code\n"
            "Option 2: Add ANTHROPIC_API_KEY to config/.env"
        )

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    kwargs: dict = {
        "model":      model or MODEL,
        "max_tokens": max_tokens,
        "messages":   [{"role": "user", "content": prompt}],
    }
    if system:
        kwargs["system"] = system

    log.info("claude.api  model=%s  prompt_chars=%d", kwargs["model"], len(prompt))
    response = client.messages.create(**kwargs)
    output = response.content[0].text.strip()
    log.info("claude.api_done  chars=%d", len(output))
    return output
