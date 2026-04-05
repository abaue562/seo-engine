"use client";

import type { Pattern } from "@/lib/api";

export default function PatternMemory({ patterns }: { patterns: Pattern[] }) {
  if (!patterns.length) {
    return (
      <div className="card">
        <h2 className="text-lg font-bold mb-2">Learning Memory</h2>
        <p className="text-sm" style={{ color: "var(--muted)" }}>No patterns learned yet. Execute tasks and run learning cycles.</p>
      </div>
    );
  }

  const winners = patterns.filter((p) => p.success_rate >= 0.6 && p.times_used >= 3);
  const killed = patterns.filter((p) => p.is_killed);

  return (
    <div>
      <h2 className="text-lg font-bold mb-3">Learning Memory</h2>
      {winners.length > 0 && (
        <div className="card mb-3">
          <h3 className="text-sm font-semibold mb-2" style={{ color: "var(--green)" }}>Proven Winners</h3>
          {winners.map((p, i) => (
            <div key={i} className="flex justify-between py-1 text-sm">
              <span>{p.pattern}</span>
              <span>{(p.success_rate * 100).toFixed(0)}% success ({p.times_used} uses)</span>
            </div>
          ))}
        </div>
      )}
      {killed.length > 0 && (
        <div className="card">
          <h3 className="text-sm font-semibold mb-2" style={{ color: "var(--red)" }}>Killed Strategies</h3>
          {killed.map((p, i) => (
            <div key={i} className="flex justify-between py-1 text-sm">
              <span>{p.pattern}</span>
              <span>{(p.success_rate * 100).toFixed(0)}% success — STOPPED</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
