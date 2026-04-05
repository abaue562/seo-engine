"use client";

interface FeedItem {
  text: string;
  type: "execution" | "event" | "learning";
  timestamp: string;
}

const typeIcon: Record<string, string> = {
  execution: "\u25B6",
  event: "\u26A1",
  learning: "\uD83E\uDDE0",
};

export default function ActivityFeed({ items }: { items: FeedItem[] }) {
  return (
    <div>
      <h2 className="text-lg font-bold mb-3">Live Activity</h2>
      <div className="card" style={{ maxHeight: 300, overflowY: "auto" }}>
        {items.length === 0 && (
          <p className="text-sm" style={{ color: "var(--muted)" }}>No activity yet. Run the system to see live updates.</p>
        )}
        {items.map((item, i) => (
          <div key={i} className="flex gap-2 py-2" style={{ borderBottom: "1px solid var(--border)" }}>
            <span>{typeIcon[item.type] || "\u2022"}</span>
            <div className="flex-1">
              <p className="text-sm">{item.text}</p>
              <p className="text-xs" style={{ color: "var(--muted)" }}>{item.timestamp}</p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
