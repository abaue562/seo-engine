"use client";

import { useState } from "react";
import type { Task, ExecResult } from "@/lib/api";

// =====================================================================
// Intelligence helpers
// =====================================================================

function whyThisRank(task: Task, allTasks: Task[]): string {
  const r: string[] = [];
  if (task.impact_score >= 9) r.push("Highest revenue impact");
  else if (task.impact_score >= 7) r.push("Strong traffic growth");
  if (task.speed_score >= 9) r.push("Results in under 7 days");
  else if (task.speed_score >= 7) r.push("Fast execution (2-4 weeks)");
  if (task.confidence_score >= 8) r.push("Proven pattern");
  if (task.ease_score >= 8) r.push("Quick win");
  if (task.type === "GBP" && task.impact_score >= 8) r.push("Directly affects map pack");
  if (task.type === "WEBSITE" && task.impact_score >= 8) r.push("Core ranking page");
  if (task.type === "AUTHORITY") r.push("Builds domain authority");
  if (allTasks.filter((t) => t.impact_score > task.impact_score).length === 0) r.push("Highest impact of all");
  return r.slice(0, 3).join(" · ") || "Best overall score";
}

function costOfInaction(task: Task): { headline: string; detail: string } {
  if (task.type === "GBP" && task.impact_score >= 8) return { headline: "Losing ~20-40 calls/month", detail: "Competitors hold map pack dominance. Every day without GBP = lost direct calls." };
  if (task.type === "GBP") return { headline: "GBP signals decay", detail: "Without activity, Google reduces visibility. Competitors gain ground weekly." };
  if (task.type === "WEBSITE" && task.impact_score >= 8) return { headline: "Zero organic traffic captured", detail: "This keyword has buying intent. Every click goes to competitors." };
  if (task.type === "WEBSITE") return { headline: "Page stays invisible", detail: "Competitor captures this keyword traffic. Gap widens weekly." };
  if (task.type === "CONTENT") return { headline: "Content gap compounds", detail: "Competitor builds topical authority first — harder to overtake later." };
  if (task.type === "AUTHORITY") return { headline: "Authority gap widens", detail: "Links compound over time. Delaying means exponentially more work later." };
  return { headline: "Opportunity window closes", detail: "Competitors move while you wait." };
}

function confidenceLabel(score: number): { text: string; color: string } {
  if (score >= 8) return { text: "Proven Pattern", color: "var(--green)" };
  if (score >= 6) return { text: "Likely Win", color: "var(--yellow)" };
  if (score >= 4) return { text: "Experimental", color: "var(--accent)" };
  return { text: "Speculative", color: "var(--red)" };
}

function confidenceExplanation(task: Task): string[] {
  const p: string[] = [];
  if (task.confidence_score >= 8) p.push("Competitors validate this approach");
  if (task.impact_score >= 8 && task.confidence_score >= 7) p.push("Proven ranking pattern in this niche");
  if (task.ease_score >= 8) p.push("Simple execution reduces failure risk");
  if (task.speed_score >= 8) p.push("Fast feedback loop — you'll know quickly");
  if (task.confidence_score < 6) p.push("Limited data — monitor results closely");
  if (p.length === 0) p.push("Moderate data backing");
  return p;
}

function unlocks(task: Task): string[] {
  const u: string[] = [];
  if (task.type === "GBP") { u.push("Map pack eligibility + local 3-pack"); u.push("Review collection pipeline"); if (task.impact_score >= 8) u.push("Direct phone call generation"); }
  if (task.type === "WEBSITE") { u.push("Organic ranking surface"); u.push("Internal linking power for other pages"); if (task.action.toLowerCase().includes("page")) u.push("Long-tail keyword capture"); }
  if (task.type === "CONTENT") { u.push("Topical authority building"); u.push("AI citation eligibility (ChatGPT/Perplexity)"); u.push("Social repurposing (TikTok/Reels)"); }
  if (task.type === "AUTHORITY") { u.push("Domain authority boost (affects ALL pages)"); u.push("Ranking ceiling increase"); u.push("Competitive moat"); }
  return u.slice(0, 3);
}

function chainedActions(task: Task): string[] {
  const c: string[] = [];
  if (task.type === "GBP") { c.push("Auto-generate weekly GBP posts"); c.push("Set up review request pipeline"); }
  if (task.type === "WEBSITE" && task.action.toLowerCase().includes("title")) { c.push("A/B test title variations"); c.push("Update internal links"); }
  if (task.type === "WEBSITE" && (task.action.toLowerCase().includes("page") || task.action.toLowerCase().includes("create"))) { c.push("Generate FAQ schema for AI"); c.push("Create supporting blog content"); c.push("Build internal links"); }
  if (task.type === "CONTENT") { c.push("Convert to TikTok + GBP post"); c.push("Add internal links to service pages"); }
  if (task.type === "AUTHORITY") { c.push("Track backlink acquisition"); c.push("Auto follow-up in 3 days"); }
  return c.slice(0, 3);
}

function impactTimeline(task: Task): { day: string; event: string }[] {
  const tl: { day: string; event: string }[] = [{ day: "Day 1", event: "Executed" }];
  if (task.type === "GBP") { tl.push({ day: "Day 3-5", event: "Indexed" }); tl.push({ day: "Day 7-14", event: "Map pack visible" }); tl.push({ day: "Day 30+", event: "Reviews build trust" }); }
  else if (task.type === "WEBSITE") { tl.push({ day: "Day 3-7", event: "Re-crawled" }); tl.push({ day: "Day 14-21", event: "Ranking moves" }); tl.push({ day: "Day 30-60", event: "Steady traffic" }); }
  else if (task.type === "CONTENT") { tl.push({ day: "Day 3-7", event: "Indexed" }); tl.push({ day: "Day 14-30", event: "Long-tail traffic" }); tl.push({ day: "Day 60+", event: "Authority compounds" }); }
  else { tl.push({ day: "Day 7-14", event: "Link indexed" }); tl.push({ day: "Day 30-45", event: "Ranking boost" }); }
  return tl;
}

function seoVsAi(task: Task): { seo: number; ai: number } {
  const a = task.action.toLowerCase() + " " + task.execution.toLowerCase();
  if (a.includes("faq") || a.includes("schema") || a.includes("structured")) return { seo: 50, ai: 70 };
  if (task.type === "CONTENT") return { seo: 60, ai: 55 };
  if (task.type === "GBP") return { seo: 85, ai: 40 };
  if (task.type === "AUTHORITY") return { seo: 90, ai: 20 };
  return { seo: 75, ai: 35 };
}

function isAIVisible(task: Task): boolean {
  const a = task.action.toLowerCase() + " " + task.execution.toLowerCase();
  return a.includes("faq") || a.includes("schema") || a.includes("structured") || a.includes("answer") || task.type === "CONTENT" || (task.type === "WEBSITE" && task.impact_score >= 8);
}

// =====================================================================
// Sub-components
// =====================================================================

function Bar({ label, value, max = 10, width = "w-20" }: { label: string; value: number; max?: number; width?: string }) {
  const pct = Math.min(100, (value / max) * 100);
  const color = value >= 8 ? "var(--green)" : value >= 6 ? "var(--yellow)" : "var(--red)";
  return (
    <div className="flex items-center gap-2 text-xs">
      <span className={width} style={{ color: "var(--muted)" }}>{label}</span>
      <div className="flex-1 h-1.5 rounded-full" style={{ background: "var(--border)" }}>
        <div className="h-full rounded-full transition-all" style={{ width: `${pct}%`, background: color }} />
      </div>
      <span className="w-6 text-right" style={{ color }}>{value}</span>
    </div>
  );
}

function PctBar({ label, pct, color }: { label: string; pct: number; color: string }) {
  return (
    <div className="flex items-center gap-2 text-xs">
      <span className="w-20" style={{ color: "var(--muted)" }}>{label}</span>
      <div className="flex-1 h-1.5 rounded-full" style={{ background: "var(--border)" }}>
        <div className="h-full rounded-full" style={{ width: `${pct}%`, background: color }} />
      </div>
      <span className="w-8 text-right" style={{ color: "var(--muted)" }}>{pct}%</span>
    </div>
  );
}

// =====================================================================
// Main
// =====================================================================

export default function TaskDetail({ task, allTasks, execResult, onExecute, executing }: {
  task: Task; allTasks: Task[]; execResult?: ExecResult; onExecute: (task: Task) => void; executing: boolean;
}) {
  const [expanded, setExpanded] = useState(false);
  const impactColor = task.impact === "high" ? "var(--red)" : "var(--yellow)";
  const modeColor = task.execution_mode === "AUTO" ? "var(--green)" : task.execution_mode === "ASSISTED" ? "var(--accent)" : "var(--muted)";
  const completed = execResult?.status === "success";
  const conf = confidenceLabel(task.confidence_score);
  const split = seoVsAi(task);
  const inaction = costOfInaction(task);

  return (
    <div className="card" style={{ borderLeft: `3px solid ${completed ? "var(--green)" : impactColor}`, opacity: completed ? 0.85 : 1 }}>
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            <span className="text-sm font-bold" style={{ color: "var(--accent)" }}>#{task.priority_rank}</span>
            <span className="badge" style={{ background: `${impactColor}20`, color: impactColor }}>{task.impact}</span>
            <span className="badge" style={{ background: `${modeColor}20`, color: modeColor }}>{task.execution_mode}</span>
            <span className="badge" style={{ background: "#1e1e2e", color: "var(--muted)" }}>{task.type}</span>
            <span className="badge" style={{
              background: task.role === "primary" ? "var(--green)20" : task.role === "supporting" ? "var(--yellow)20" : "var(--accent)20",
              color: task.role === "primary" ? "var(--green)" : task.role === "supporting" ? "var(--yellow)" : "var(--accent)",
            }}>{(task.role || "primary").toUpperCase()}</span>
            {isAIVisible(task) && <span className="badge" style={{ background: "#8b5cf620", color: "#a78bfa" }}>AI VISIBLE</span>}
            <span className="badge" style={{ background: `${conf.color}20`, color: conf.color }}>{conf.text}</span>
            <span className="text-xs font-bold" style={{ color: "var(--accent)" }}>{task.total_score.toFixed(1)}</span>
          </div>
          <h3 className="font-bold">{task.action}</h3>
          <p className="text-sm mt-1" style={{ color: "var(--muted)" }}>{task.why}</p>
          <p className="text-xs mt-2" style={{ color: "#a78bfa" }}>Why #{task.priority_rank}: {whyThisRank(task, allTasks)}</p>
        </div>
        <div className="flex flex-col gap-2 items-end">
          {!completed ? (
            <button className="btn btn-primary" onClick={() => onExecute(task)} disabled={executing}>
              {executing ? "Running..." : task.execution_mode === "AUTO" ? "Execute" : task.execution_mode === "ASSISTED" ? "Approve" : "View Steps"}
            </button>
          ) : (
            <span className="badge" style={{ background: "var(--green)20", color: "var(--green)", padding: "6px 14px" }}>Completed</span>
          )}
          <button className="btn btn-ghost text-xs" onClick={() => setExpanded(!expanded)}>{expanded ? "Less" : "Details"}</button>
        </div>
      </div>

      {/* Score bars */}
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 mt-3">
        <Bar label="Impact" value={task.impact_score} />
        <Bar label="Ease" value={task.ease_score} />
        <Bar label="Speed" value={task.speed_score} />
        <Bar label="Confidence" value={task.confidence_score} />
      </div>

      <div className="flex gap-4 mt-2 text-xs" style={{ color: "var(--muted)" }}>
        <span>Result: {task.estimated_result}</span>
        <span>Time: {task.time_to_result}</span>
      </div>

      {/* Chained preview (collapsed) */}
      {chainedActions(task).length > 0 && !expanded && (
        <p className="text-xs mt-2" style={{ color: "var(--green)" }}>Also triggers: {chainedActions(task).join(" → ")}</p>
      )}

      {/* ---- Expanded ---- */}
      {expanded && (
        <div className="mt-3 pt-3 space-y-4" style={{ borderTop: "1px solid var(--border)" }}>
          {/* Cost of inaction */}
          <div className="p-3 rounded-lg" style={{ background: "#ef444410" }}>
            <p className="text-xs font-bold" style={{ color: "var(--red)" }}>If you do nothing: {inaction.headline}</p>
            <p className="text-xs mt-1" style={{ color: "#f87171" }}>{inaction.detail}</p>
          </div>

          {/* Timeline */}
          <div>
            <p className="text-xs font-semibold mb-2">Impact Timeline</p>
            <div className="flex gap-1">
              {impactTimeline(task).map((s, i) => (
                <div key={i} className="flex-1 p-2 rounded text-center" style={{ background: "var(--bg)" }}>
                  <p className="text-xs font-bold" style={{ color: "var(--accent)" }}>{s.day}</p>
                  <p className="text-xs mt-0.5" style={{ color: "var(--muted)" }}>{s.event}</p>
                </div>
              ))}
            </div>
          </div>

          {/* SEO vs AI split */}
          <div>
            <p className="text-xs font-semibold mb-1">Impact Breakdown</p>
            <PctBar label="SEO" pct={split.seo} color="var(--green)" />
            <PctBar label="AI Visibility" pct={split.ai} color="#a78bfa" />
          </div>

          {/* Confidence */}
          <div>
            <p className="text-xs font-semibold" style={{ color: conf.color }}>Confidence: {conf.text}</p>
            <ul className="text-xs mt-1 space-y-0.5">
              {confidenceExplanation(task).map((r, i) => <li key={i} style={{ color: "var(--muted)" }}>- {r}</li>)}
            </ul>
          </div>

          {/* Unlocks */}
          <div>
            <p className="text-xs font-semibold" style={{ color: "var(--green)" }}>This unlocks:</p>
            <ul className="text-xs mt-1 space-y-0.5">
              {unlocks(task).map((u, i) => <li key={i} style={{ color: "var(--muted)" }}>+ {u}</li>)}
            </ul>
          </div>

          {/* Chained */}
          {chainedActions(task).length > 0 && (
            <div>
              <p className="text-xs font-semibold" style={{ color: "var(--accent)" }}>Also triggers:</p>
              <ul className="text-xs mt-1 space-y-0.5">
                {chainedActions(task).map((c, i) => <li key={i} style={{ color: "var(--muted)" }}>&rarr; {c}</li>)}
              </ul>
            </div>
          )}

          {/* Execution preview */}
          <div>
            <p className="text-xs font-semibold mb-1">Execution Preview</p>
            <div className="text-xs whitespace-pre-wrap p-3 rounded-lg" style={{ background: "var(--bg)", color: "var(--muted)", maxHeight: 250, overflowY: "auto" }}>
              {task.execution}
            </div>
          </div>
        </div>
      )}

      {/* ---- Result ---- */}
      {execResult && (
        <div className="mt-3 pt-3" style={{ borderTop: "1px solid var(--border)" }}>
          <div className="flex items-center gap-2 mb-2">
            <span className="badge" style={{ background: completed ? "var(--green)20" : "var(--red)20", color: completed ? "var(--green)" : "var(--red)" }}>{execResult.status}</span>
            <span className="text-xs" style={{ color: "var(--muted)" }}>ID: {execResult.task_id}</span>
            {completed && <span className="text-xs" style={{ color: "var(--green)" }}>Content generated — ready to deploy</span>}
          </div>
          <pre className="text-xs overflow-auto p-3 rounded-lg" style={{ background: "var(--bg)", maxHeight: 200 }}>
            {JSON.stringify(execResult.output, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}
