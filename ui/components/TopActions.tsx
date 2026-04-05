"use client";

import type { Task } from "@/lib/api";

const impactBadge = (impact: string) => {
  const cls = impact === "high" ? "badge-high" : "badge-medium";
  return <span className={`badge ${cls}`}>{impact}</span>;
};

const modeBadge = (mode: string) => {
  const cls = mode === "AUTO" ? "badge-auto" : mode === "ASSISTED" ? "badge-assisted" : "badge-manual";
  return <span className={`badge ${cls}`}>{mode}</span>;
};

export default function TopActions({
  tasks,
  onExecute,
}: {
  tasks: Task[];
  onExecute: (task: Task) => void;
}) {
  if (!tasks.length) {
    return (
      <div className="card">
        <h2 className="text-lg font-bold mb-2">Top Actions</h2>
        <p style={{ color: "var(--muted)" }}>No actions yet. Run analysis to generate tasks.</p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <h2 className="text-lg font-bold">Top Actions</h2>
      {tasks.slice(0, 5).map((task, i) => (
        <div key={i} className="card">
          <div className="flex items-start justify-between gap-3">
            <div className="flex-1">
              <div className="flex items-center gap-2 mb-1">
                <span className="text-xs font-bold" style={{ color: "var(--accent)" }}>#{task.priority_rank}</span>
                {impactBadge(task.impact)}
                {modeBadge(task.execution_mode)}
                <span className="badge" style={{ background: "#1e1e2e", color: "var(--muted)" }}>{task.type}</span>
              </div>
              <h3 className="font-bold text-base">{task.action}</h3>
              <p className="text-sm mt-1" style={{ color: "var(--muted)" }}>{task.why}</p>
              <div className="flex gap-4 mt-2 text-xs" style={{ color: "var(--muted)" }}>
                <span>Score: {task.total_score.toFixed(1)}</span>
                <span>Result: {task.estimated_result}</span>
                <span>Time: {task.time_to_result}</span>
              </div>
            </div>
            <button className="btn btn-primary" onClick={() => onExecute(task)}>
              {task.execution_mode === "AUTO" ? "Execute" : task.execution_mode === "ASSISTED" ? "Approve" : "View"}
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}
