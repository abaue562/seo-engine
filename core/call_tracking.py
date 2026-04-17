"""
Call tracking layer: dynamic number insertion (DNI), call log, source attribution.
No third-party API required — uses lightweight number pool stored in SQLite.
"""
from __future__ import annotations
import hashlib
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)
_DB = "data/storage/seo_engine.db"


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS tracking_numbers (
            id          TEXT PRIMARY KEY,
            business_id TEXT NOT NULL,
            number      TEXT NOT NULL,
            label       TEXT DEFAULT '',
            source      TEXT DEFAULT 'organic',
            medium      TEXT DEFAULT '',
            campaign    TEXT DEFAULT '',
            active      INTEGER DEFAULT 1,
            created_at  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_tn_biz ON tracking_numbers(business_id, source);

        CREATE TABLE IF NOT EXISTS call_log (
            id              TEXT PRIMARY KEY,
            business_id     TEXT NOT NULL,
            tracking_number TEXT NOT NULL,
            caller_number   TEXT DEFAULT '',
            source          TEXT DEFAULT '',
            medium          TEXT DEFAULT '',
            campaign        TEXT DEFAULT '',
            duration_sec    INTEGER DEFAULT 0,
            answered        INTEGER DEFAULT 0,
            recording_url   TEXT DEFAULT '',
            notes           TEXT DEFAULT '',
            created_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_cl_biz ON call_log(business_id, created_at);

        CREATE TABLE IF NOT EXISTS conversion_events (
            id              TEXT PRIMARY KEY,
            business_id     TEXT NOT NULL,
            event_type      TEXT NOT NULL,
            source          TEXT DEFAULT '',
            medium          TEXT DEFAULT '',
            campaign        TEXT DEFAULT '',
            page_url        TEXT DEFAULT '',
            value           REAL DEFAULT 0,
            metadata        TEXT DEFAULT '{}',
            created_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_ce_biz ON conversion_events(business_id, event_type, created_at);
    """)
    c.commit()
    return c


def add_tracking_number(
    business_id: str,
    number: str,
    label: str = "",
    source: str = "organic",
    medium: str = "",
    campaign: str = "",
) -> dict:
    nid = hashlib.md5(f"{business_id}:{number}".encode()).hexdigest()[:12]
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("""
            INSERT INTO tracking_numbers (id, business_id, number, label, source, medium, campaign, created_at)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET label=excluded.label, source=excluded.source,
                medium=excluded.medium, campaign=excluded.campaign
        """, [nid, business_id, number, label, source, medium, campaign, now])
    return {"id": nid, "number": number, "source": source}


def get_tracking_numbers(business_id: str, source: str = "") -> list[dict]:
    with _conn() as c:
        if source:
            rows = c.execute(
                "SELECT * FROM tracking_numbers WHERE business_id=? AND source=? AND active=1",
                [business_id, source]
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM tracking_numbers WHERE business_id=? AND active=1",
                [business_id]
            ).fetchall()
    return [dict(r) for r in rows]


def log_call(
    business_id: str,
    tracking_number: str,
    caller_number: str = "",
    source: str = "",
    medium: str = "",
    campaign: str = "",
    duration_sec: int = 0,
    answered: bool = False,
    recording_url: str = "",
    notes: str = "",
) -> dict:
    cid = hashlib.md5(f"{business_id}:{tracking_number}:{datetime.now().isoformat()}".encode()).hexdigest()[:16]
    now = datetime.now(timezone.utc).isoformat()

    # Auto-resolve source from tracking number if not provided
    if not source and tracking_number:
        with _conn() as c:
            row = c.execute(
                "SELECT source, medium, campaign FROM tracking_numbers WHERE business_id=? AND number=?",
                [business_id, tracking_number]
            ).fetchone()
            if row:
                source = row["source"]
                medium = medium or row["medium"]
                campaign = campaign or row["campaign"]

    with _conn() as c:
        c.execute("""
            INSERT INTO call_log
                (id, business_id, tracking_number, caller_number, source, medium, campaign,
                 duration_sec, answered, recording_url, notes, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, [cid, business_id, tracking_number, caller_number, source, medium, campaign,
              duration_sec, int(answered), recording_url, notes, now])
    log.info("call_log  biz=%s  src=%s  answered=%s  dur=%ds", business_id, source, answered, duration_sec)
    return {"id": cid, "source": source, "answered": answered}


def log_conversion(
    business_id: str,
    event_type: str,
    source: str = "",
    medium: str = "",
    campaign: str = "",
    page_url: str = "",
    value: float = 0.0,
    metadata: dict | None = None,
) -> dict:
    eid = hashlib.md5(f"{business_id}:{event_type}:{datetime.now().isoformat()}".encode()).hexdigest()[:16]
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("""
            INSERT INTO conversion_events
                (id, business_id, event_type, source, medium, campaign, page_url, value, metadata, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, [eid, business_id, event_type, source, medium, campaign, page_url,
              value, json.dumps(metadata or {}), now])
    return {"id": eid, "event_type": event_type}


def get_call_stats(business_id: str, days: int = 30) -> dict:
    with _conn() as c:
        rows = c.execute("""
            SELECT source, COUNT(*) as total, SUM(answered) as answered,
                   AVG(duration_sec) as avg_dur
            FROM call_log
            WHERE business_id=?
              AND created_at >= datetime('now', ? || ' days')
            GROUP BY source
            ORDER BY total DESC
        """, [business_id, f"-{days}"]).fetchall()

        total_row = c.execute("""
            SELECT COUNT(*) as total, SUM(answered) as answered
            FROM call_log WHERE business_id=?
              AND created_at >= datetime('now', ? || ' days')
        """, [business_id, f"-{days}"]).fetchone()

    by_source = []
    for r in rows:
        by_source.append({
            "source": r["source"],
            "total_calls": r["total"],
            "answered": r["answered"] or 0,
            "answer_rate": round((r["answered"] or 0) / r["total"] * 100, 1),
            "avg_duration_sec": round(r["avg_dur"] or 0, 1),
        })

    return {
        "business_id": business_id,
        "days": days,
        "total_calls": total_row["total"] or 0,
        "total_answered": total_row["answered"] or 0,
        "answer_rate": round((total_row["answered"] or 0) / max(total_row["total"] or 1, 1) * 100, 1),
        "by_source": by_source,
    }


def get_conversion_stats(business_id: str, days: int = 30) -> dict:
    with _conn() as c:
        rows = c.execute("""
            SELECT event_type, source, COUNT(*) as total, SUM(value) as total_value
            FROM conversion_events
            WHERE business_id=?
              AND created_at >= datetime('now', ? || ' days')
            GROUP BY event_type, source
            ORDER BY total DESC
        """, [business_id, f"-{days}"]).fetchall()

    events: dict[str, dict] = {}
    for r in rows:
        et = r["event_type"]
        if et not in events:
            events[et] = {"total": 0, "total_value": 0.0, "by_source": []}
        events[et]["total"] += r["total"]
        events[et]["total_value"] += r["total_value"] or 0
        events[et]["by_source"].append({
            "source": r["source"],
            "count": r["total"],
            "value": r["total_value"] or 0,
        })

    return {"business_id": business_id, "days": days, "events": events}


def inject_dni_script(html: str, business_id: str, default_number: str) -> str:
    """
    Inject Dynamic Number Insertion JS into HTML.
    Reads UTM params from URL, swaps phone number display based on source.
    Tracking numbers must be pre-loaded via add_tracking_number().
    """
    numbers = get_tracking_numbers(business_id)
    number_map = {n["source"]: n["number"] for n in numbers}
    number_map_json = json.dumps(number_map)

    script = f"""<script>
(function(){{
  var map = {number_map_json};
  var def_num = "{default_number}";
  function getParam(p){{
    var u = new URLSearchParams(window.location.search);
    return u.get(p) || '';
  }}
  var src = getParam('utm_source') || getParam('source') || document.referrer;
  if(src.indexOf('google')>-1) src='google';
  else if(src.indexOf('bing')>-1) src='bing';
  else if(src.indexOf('facebook')>-1 || src.indexOf('fb')>-1) src='facebook';
  var num = map[src] || map['organic'] || def_num;
  document.addEventListener('DOMContentLoaded', function(){{
    document.querySelectorAll('a[href^="tel:"], .phone, [data-phone]').forEach(function(el){{
      if(el.tagName==='A') el.href='tel:'+num.replace(/[^0-9+]/g,'');
      el.textContent = num;
    }});
  }});
  // Log call intent
  document.addEventListener('click', function(e){{
    var t = e.target.closest('a[href^="tel:"]');
    if(t){{
      fetch('/conversion/event', {{
        method:'POST',
        headers:{{'Content-Type':'application/json'}},
        body: JSON.stringify({{
          business_id: '{business_id}',
          event_type: 'call_click',
          source: src,
          page_url: window.location.href,
          metadata: {{number: num}}
        }})
      }}).catch(function(){{}});
    }}
  }});
}})();
</script>"""

    if "</body>" in html:
        return html.replace("</body>", script + "\n</body>", 1)
    return html + script
