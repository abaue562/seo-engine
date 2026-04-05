"use client";

import type { Task } from "@/lib/api";

export default function DecisionLog({ tasks }: { tasks: Task[] }) {
  return (
    <div>
      <h2 className="text-lg font-bold mb-3">AI Decisions</h2>
      <div className="space-y-2">
        {tasks.slice(0, 5).map((task, i) => (
          <div key={i} className="card" style={{ padding: "1rem" }}>
            <p className="text-sm font-semibold">{task.action}</p>
            <p className="text-xs mt-1" style={{ color: "var(--muted)" }}>
              Target: {task.target}
            </p>
            <p className="text-xs mt-1" style={{ color: "var(--muted)" }}>
              Why: {task.why}
            </p>
            <div className="flex gap-3 mt-2 text-xs" style={{ color: "var(--muted)" }}>
              <span>Impact: {task.impact_score}/10</span>
              <span>Ease: {task.ease_score}/10</span>
              <span>Speed: {task.speed_score}/10</span>
              <span>Confidence: {task.confidence_score}/10</span>
            </div>
          </div>
        ))}
        {!tasks.length && (
          <p className="text-sm" style={{ color: "var(--muted)" }}>No decisions yet.</p>
        )}
      </div>
    </div>
  );
}
