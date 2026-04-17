"use client";
import { useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";

interface HistoryPoint {
  position: number;
  at: string;
}

interface KeywordRanking {
  keyword: string;
  current_position: number | null;
  delta_7d: number | null;
  delta_30d: number | null;
  best_ever: number | null;
  history: HistoryPoint[];
}

interface RankingsData {
  business_id: string;
  days: number;
  keywords: KeywordRanking[];
}

function DeltaBadge({ delta }: { delta: number | null }) {
  if (delta === null || delta === 0) {
    return <span className="text-gray-400 text-sm">—</span>;
  }
  // Negative delta = position number went down = improved ranking
  if (delta < 0) {
    return (
      <span className="inline-flex items-center gap-0.5 text-green-600 font-semibold text-sm">
        ↑ {Math.abs(delta)}
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-0.5 text-red-500 font-semibold text-sm">
      ↓ {delta}
    </span>
  );
}

function PositionBadge({ pos }: { pos: number | null }) {
  if (pos === null) return <span className="text-gray-400">—</span>;
  if (pos <= 3)
    return (
      <span className="inline-block bg-green-100 text-green-800 font-bold px-2 py-0.5 rounded-full text-sm">
        #{pos}
      </span>
    );
  if (pos <= 10)
    return (
      <span className="inline-block bg-blue-100 text-blue-800 font-bold px-2 py-0.5 rounded-full text-sm">
        #{pos}
      </span>
    );
  if (pos <= 20)
    return (
      <span className="inline-block bg-yellow-100 text-yellow-800 font-bold px-2 py-0.5 rounded-full text-sm">
        #{pos}
      </span>
    );
  return (
    <span className="inline-block bg-gray-100 text-gray-600 font-bold px-2 py-0.5 rounded-full text-sm">
      #{pos}
    </span>
  );
}

export default function RankingsPage() {
  const params = useSearchParams();
  const businessId = params.get("id") || "";
  const [days, setDays] = useState(30);
  const [data, setData] = useState<RankingsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!businessId) {
      setLoading(false);
      setError("No business ID provided in URL (?id=...)");
      return;
    }
    setLoading(true);
    fetch(
      `/seo/api/tenant/rankings?business_id=${encodeURIComponent(businessId)}&days=${days}`
    )
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d) => {
        setData(d);
        setLoading(false);
      })
      .catch((e) => {
        setError(e.message);
        setLoading(false);
      });
  }, [businessId, days]);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="text-gray-500 text-lg animate-pulse">Loading rankings...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="text-red-500 text-lg">{error}</div>
      </div>
    );
  }

  if (!data || data.keywords.length === 0) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="text-center">
          <div className="text-4xl mb-4">📊</div>
          <h2 className="text-2xl font-bold text-gray-700 mb-2">Rank tracking is initializing</h2>
          <p className="text-gray-500">Check back in 24 hours for your first ranking data.</p>
        </div>
      </div>
    );
  }

  const inTop10 = data.keywords.filter((k) => k.current_position !== null && k.current_position <= 10).length;
  const improved = data.keywords.filter((k) => k.delta_7d !== null && k.delta_7d < 0).length;

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-5xl mx-auto space-y-6">

        {/* Header */}
        <div className="bg-white rounded-xl shadow p-6">
          <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
            <div>
              <h1 className="text-2xl font-bold text-gray-800 mb-1">Keyword Rankings</h1>
              <p className="text-gray-500">
                <span className="font-semibold text-indigo-600">{inTop10}</span> keywords in top 10 ·{" "}
                <span className="font-semibold text-green-600">{improved}</span> improved this week
              </p>
            </div>
            <div className="flex gap-2">
              {[7, 30, 90].map((d) => (
                <button
                  key={d}
                  onClick={() => setDays(d)}
                  className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                    days === d
                      ? "bg-indigo-600 text-white"
                      : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                  }`}
                >
                  {d}d
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Table */}
        <div className="bg-white rounded-xl shadow overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="text-left px-6 py-3 font-semibold text-gray-600">Keyword</th>
                  <th className="text-center px-4 py-3 font-semibold text-gray-600">Position</th>
                  <th className="text-center px-4 py-3 font-semibold text-gray-600">7d Change</th>
                  <th className="text-center px-4 py-3 font-semibold text-gray-600">30d Change</th>
                  <th className="text-center px-4 py-3 font-semibold text-gray-600">Best Ever</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {data.keywords.map((kw, i) => (
                  <tr key={i} className="hover:bg-gray-50 transition-colors">
                    <td className="px-6 py-3 font-medium text-gray-800">{kw.keyword}</td>
                    <td className="px-4 py-3 text-center">
                      <PositionBadge pos={kw.current_position} />
                    </td>
                    <td className="px-4 py-3 text-center">
                      <DeltaBadge delta={kw.delta_7d} />
                    </td>
                    <td className="px-4 py-3 text-center">
                      <DeltaBadge delta={kw.delta_30d} />
                    </td>
                    <td className="px-4 py-3 text-center text-gray-500">
                      {kw.best_ever !== null ? `#${kw.best_ever}` : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
