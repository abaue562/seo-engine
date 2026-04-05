"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";

export default function SystemStatus() {
  const [status, setStatus] = useState<{
    online: boolean;
    mode: string;
    lastCheck: string;
  }>({ online: false, mode: "...", lastCheck: "" });

  useEffect(() => {
    const check = async () => {
      try {
        const h = await api.health();
        setStatus({
          online: true,
          mode: h.claude_mode || "unknown",
          lastCheck: new Date().toLocaleTimeString(),
        });
      } catch {
        setStatus((s) => ({ ...s, online: false, lastCheck: new Date().toLocaleTimeString() }));
      }
    };
    check();
    const interval = setInterval(check, 30_000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="flex items-center gap-3 text-xs" style={{ color: "var(--muted)" }}>
      <div className="flex items-center gap-1.5">
        <div
          className="w-2 h-2 rounded-full"
          style={{ background: status.online ? "var(--green)" : "var(--red)" }}
        />
        <span>{status.online ? "System Online" : "System Offline"}</span>
      </div>
      {status.online && (
        <>
          <span>Claude: {status.mode.toUpperCase()}</span>
          <span>Checked: {status.lastCheck}</span>
        </>
      )}
    </div>
  );
}
