"use client";

import type { Task } from "@/lib/api";

export default function CompoundImpact({ tasks }: { tasks: Task[] }) {
  if (tasks.length < 2) return null;

  const top3 = tasks.slice(0, 3);
  const avgImpact = top3.reduce((s, t) => s + t.impact_score, 0) / top3.length;
  const avgSpeed = top3.reduce((s, t) => s + t.speed_score, 0) / top3.length;
  const avgConf = top3.reduce((s, t) => s + t.confidence_score, 0) / top3.length;

  // Estimate compounding effect
  const hasGBP = top3.some((t) => t.type === "GBP");
  const hasWebsite = top3.some((t) => t.type === "WEBSITE");
  const hasAuthority = top3.some((t) => t.type === "AUTHORITY");
  const hasContent = top3.some((t) => t.type === "CONTENT");

  const channels = [hasGBP && "Map Pack", hasWebsite && "Organic", hasAuthority && "Authority", hasContent && "Content"]
    .filter(Boolean);

  const positionGain = Math.round(avgImpact * 0.5);
  const callIncrease = Math.round(avgImpact * avgConf * 0.8);

  return (
    <div className="card" style={{ borderLeft: "3px solid var(--accent)" }}>
      <h3 className="text-sm font-bold mb-2" style={{ color: "var(--accent)" }}>
        Stacked Impact — Top {top3.length} Tasks Combined
      </h3>
      <div className="grid grid-cols-3 gap-3 text-center">
        <div>
          <p className="text-xl font-bold" style={{ color: "var(--green)" }}>+{positionGain}-{positionGain + 2}</p>
          <p className="text-xs" style={{ color: "var(--muted)" }}>Ranking positions</p>
        </div>
        <div>
          <p className="text-xl font-bold" style={{ color: "var(--green)" }}>+{callIncrease}-{callIncrease + 20}%</p>
          <p className="text-xs" style={{ color: "var(--muted)" }}>More calls/traffic</p>
        </div>
        <div>
          <p className="text-xl font-bold" style={{ color: "var(--accent)" }}>{channels.length}</p>
          <p className="text-xs" style={{ color: "var(--muted)" }}>Ranking surfaces</p>
        </div>
      </div>
      <p className="text-xs mt-2" style={{ color: "var(--muted)" }}>
        Channels affected: {channels.join(" + ")}
      </p>
      {avgSpeed >= 7 && (
        <p className="text-xs mt-1" style={{ color: "var(--green)" }}>
          Fast execution — results expected within 2-4 weeks
        </p>
      )}
    </div>
  );
}
