"""Unified Claude caller — supports CLI (claude command) and API.

Default: CLI mode — uses your Claude Code subscription, no API key needed.
Fallback: API mode — uses ANTHROPIC_API_KEY from config.

CLI mode runs `claude` as a subprocess with --print flag for non-interactive output.
This means NO API key needed — uses your existing Claude CLI auth.
"""

from __future__ import annotations

import json
import logging
import subprocess
import shutil

from config.settings import ANTHROPIC_API_KEY, MODEL

log = logging.getLogger(__name__)

# Auto-detect: prefer CLI if `claude` is available
def _find_claude_cli() -> str | None:
    """Find claude CLI — check PATH first, then common install locations."""
    import os
    # 1. Check PATH
    found = shutil.which("claude")
    if found:
        return found
    # 2. Check common Windows install locations
    home = os.path.expanduser("~")
    candidates = [
        os.path.join(home, "AppData", "Roaming", "npm", "claude.cmd"),
        os.path.join(home, "AppData", "Local", "npm-cache", "_npx"),  # npx cache parent
        os.path.join(home, ".claude", "local", "bin", "claude"),
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
        # Search npx cache directories (no depth limit — hash dirs can be deep)
        if os.path.isdir(c):
            for root, dirs, files in os.walk(c):
                # Prefer .cmd on Windows
                for f in files:
                    if f == "claude.cmd":
                        return os.path.join(root, f)
                for f in files:
                    if f in ("claude.exe", "claude"):
                        return os.path.join(root, f)
    # 3. Last resort: known npx cache path on this machine
    fallback = os.path.join(
        home, "AppData", "Local", "npm-cache", "_npx",
        "becf7b9e49303068", "node_modules", ".bin", "claude.cmd"
    )
    if os.path.isfile(fallback):
        return fallback
    return None

_CLI_PATH = _find_claude_cli()
USE_CLI = _CLI_PATH is not None


def call_claude(
    prompt: str,
    system: str = "",
    max_tokens: int = 4096,
    model: str | None = None,
    force_api: bool = False,
) -> str:
    """Call Claude via CLI or API. Returns raw text response.

    Args:
        prompt: The user message to send
        system: System prompt (optional)
        max_tokens: Max tokens in response
        model: Model override
        force_api: Force API mode even if CLI is available
    """
    if USE_CLI and not force_api:
        return _call_cli(prompt, system, max_tokens, model)
    else:
        return _call_api(prompt, system, max_tokens, model)


def _call_cli(prompt: str, system: str, max_tokens: int, model: str | None) -> str:
    """Call Claude via the CLI subprocess."""
    full_prompt = prompt

    cmd = [_CLI_PATH, "--print", "--max-turns", "3"]

    # Add model flag — default to sonnet for speed
    cmd.extend(["--model", model or "sonnet"])

    # Pass system prompt via --system-prompt flag (keeps it separate from user input)
    if system:
        cmd.extend(["--system-prompt", system])

    # On Windows, .cmd files need shell=True
    use_shell = _CLI_PATH.endswith(".cmd")

    log.info("claude.cli  cmd=%s  prompt_len=%d  shell=%s", " ".join(cmd[:3]), len(full_prompt), use_shell)

    try:
        result = subprocess.run(
            cmd,
            input=full_prompt,
            capture_output=True,
            text=True,
            timeout=300,
            shell=use_shell,
            encoding="utf-8",
            errors="replace",
        )

        if result.returncode != 0:
            stderr = result.stderr[:500] if result.stderr else ""
            log.error("claude.cli_error  code=%d  stderr=%s", result.returncode, stderr)
            raise RuntimeError(f"Claude CLI failed (code {result.returncode}): {stderr}")

        output = result.stdout.strip()
        log.info("claude.cli_done  output_len=%d", len(output))
        return output

    except subprocess.TimeoutExpired:
        log.error("claude.cli_timeout  prompt_len=%d", len(full_prompt))
        raise RuntimeError("Claude CLI timed out after 300s — prompt may be too large or CLI is busy")
    except FileNotFoundError:
        log.error("claude.cli_not_found  path=%s", _CLI_PATH)
        # Fall back to API
        return _call_api(prompt, system, max_tokens, model)


def _call_api(prompt: str, system: str, max_tokens: int, model: str | None) -> str:
    """Call Claude via the Anthropic API."""
    import anthropic

    if not ANTHROPIC_API_KEY:
        raise RuntimeError(
            "No Claude CLI found and no ANTHROPIC_API_KEY set. "
            "Either install Claude CLI or add your API key to config/.env"
        )

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    kwargs: dict = {
        "model": model or MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        kwargs["system"] = system

    response = client.messages.create(**kwargs)
    output = response.content[0].text.strip()
    log.info("claude.api_done  output_len=%d", len(output))
    return output


def call_claude_json(
    prompt: str,
    system: str = "",
    max_tokens: int = 4096,
    model: str | None = None,
) -> dict | list:
    """Call Claude and parse JSON response. Handles markdown fences and trailing text."""
    raw = call_claude(prompt, system, max_tokens, model)

    # Strip markdown code fences
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    # Seek to first JSON token (object or array)
    start = -1
    for i, ch in enumerate(raw):
        if ch in "{[":
            start = i
            break
    if start > 0:
        raw = raw[start:]

    try:
        data, _ = json.JSONDecoder().raw_decode(raw)
        return data
    except json.JSONDecodeError:
        log.error("claude.json_parse_fail  raw=%s", raw[:200])
        return {}


def call_claude_raw(
    model: str | None = None,
    max_tokens: int = 4096,
    messages: list[dict] | None = None,
    system: str = "",
    **kwargs,
) -> type("Response", (), {"content": [type("Block", (), {"text": ""})]})():
    """Compatibility wrapper — accepts the old anthropic API signature.

    Used by handlers that were refactored from self.client.messages.create().
    Returns an object with .content[0].text like the anthropic SDK does.
    """
    # Extract the user prompt from messages
    prompt_parts = []
    if messages:
        for msg in messages:
            if msg.get("role") == "user":
                prompt_parts.append(msg.get("content", ""))

    prompt = "\n\n".join(prompt_parts)
    raw = call_claude(prompt, system=system, max_tokens=max_tokens, model=model)

    # Return an object mimicking anthropic's response shape
    class TextBlock:
        def __init__(self, text):
            self.text = text

    class Response:
        def __init__(self, text):
            self.content = [TextBlock(text)]

    return Response(raw)


def get_mode() -> str:
    """Return current mode: 'cli' or 'api'."""
    return "cli" if USE_CLI and _CLI_PATH else "api"
