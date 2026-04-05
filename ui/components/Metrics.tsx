"use client";

interface MetricData {
  label: string;
  value: string | number;
  change?: string;
  positive?: boolean;
}

export default function Metrics({ metrics }: { metrics: MetricData[] }) {
  return (
    <div>
      <h2 className="text-lg font-bold mb-3">Growth Metrics</h2>
      <div className="grid grid-cols-2 gap-3">
        {metrics.map((m, i) => (
          <div key={i} className="card">
            <p className="text-xs uppercase" style={{ color: "var(--muted)" }}>{m.label}</p>
            <p className="text-2xl font-bold mt-1">{m.value}</p>
            {m.change && (
              <p className="text-xs mt-1" style={{ color: m.positive ? "var(--green)" : "var(--red)" }}>
                {m.change}
              </p>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
