"use client";

import { useState } from "react";

interface Automation {
  id: string;
  label: string;
  enabled: boolean;
}

const defaultAutomations: Automation[] = [
  { id: "daily_sync", label: "Daily Data Sync", enabled: true },
  { id: "weekly_optimize", label: "Weekly Optimization", enabled: true },
  { id: "auto_gbp", label: "Auto GBP Posts", enabled: false },
  { id: "auto_content", label: "Auto Content", enabled: false },
  { id: "auto_reviews", label: "Auto Review Responses", enabled: false },
  { id: "learning_loop", label: "Weekly Learning Cycle", enabled: true },
];

export default function AutomationPanel() {
  const [automations, setAutomations] = useState(defaultAutomations);

  const toggle = (id: string) => {
    setAutomations((prev) =>
      prev.map((a) => (a.id === id ? { ...a, enabled: !a.enabled } : a))
    );
  };

  return (
    <div>
      <h2 className="text-lg font-bold mb-3">Automations</h2>
      <div className="card space-y-3">
        {automations.map((auto) => (
          <div key={auto.id} className="flex items-center justify-between">
            <span className="text-sm">{auto.label}</span>
            <button
              className="w-12 h-6 rounded-full transition-colors"
              style={{
                background: auto.enabled ? "var(--green)" : "var(--border)",
              }}
              onClick={() => toggle(auto.id)}
            >
              <div
                className="w-5 h-5 rounded-full bg-white transition-transform"
                style={{
                  transform: auto.enabled ? "translateX(26px)" : "translateX(2px)",
                }}
              />
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
