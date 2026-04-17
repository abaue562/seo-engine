"use client";
import { useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";

interface IndexingInfo {
  indexed: number;
  pending: number;
  failed: number;
}

interface RankWin {
  keyword: string;
  from: number;
  to: number;
  delta: number;
}

interface TopPage {
  url: string;
  keyword: string;
  published_at: string;
}

interface Summary {
  business_id: string;
  published_this_week: number;
  published_all_time: number;
  rank_wins: RankWin[];
  indexing: IndexingInfo;
  top_pages: TopPage[];
  wins: string[];
}

export default function TenantDashboard() {
  const params = useSearchParams();
  const businessId = params.get("id") || "";
  const [data, setData] = useState<Summary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!businessId) {
      setLoading(false);
      setError("No business ID provided in URL (?id=...)");
      return;
    }
    fetch(`/seo/api/tenant/summary?business_id=${encodeURIComponent(businessId)}`)
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
  }, [businessId]);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="text-gray-500 text-lg animate-pulse">Loading your SEO dashboard...</div>
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

  if (!data || (data.published_all_time === 0 && data.published_this_week === 0)) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="text-center">
          <div className="text-4xl mb-4">🚀</div>
          <h2 className="text-2xl font-bold text-gray-700 mb-2">Setting up your SEO engine</h2>
          <p className="text-gray-500">First content ships soon — check back shortly.</p>
        </div>
      </div>
    );
  }

  const topInTop10 = data.rank_wins.filter((r) => r.to <= 10).length;

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-5xl mx-auto space-y-6">

        {/* Hero */}
        <div className="bg-indigo-600 rounded-2xl p-8 text-white shadow-lg">
          <h1 className="text-3xl font-bold mb-2">Your SEO Results This Week</h1>
          <p className="text-indigo-100 text-lg">
            We published{" "}
            <span className="font-semibold text-white">{data.published_this_week} pages</span>,
            ranked{" "}
            <span className="font-semibold text-white">{topInTop10} new keywords</span> in the top 10,
            and{" "}
            <span className="font-semibold text-white">{data.indexing.indexed} pages</span> are indexed.
          </p>
        </div>

        {/* Wins list */}
        {data.wins.length > 0 && (
          <div className="bg-green-50 border border-green-200 rounded-xl p-5">
            <h2 className="text-lg font-semibold text-green-800 mb-3">Wins</h2>
            <ul className="space-y-1">
              {data.wins.map((w, i) => (
                <li key={i} className="text-green-700 flex items-start gap-2">
                  <span className="mt-1 text-green-500">✓</span>
                  {w}
                </li>
              ))}
            </ul>
          </div>
        )}

        {/* 3 Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">

          {/* Content Shipped */}
          <div className="bg-white rounded-xl shadow p-5">
            <h2 className="text-lg font-semibold text-gray-800 mb-1">Content Shipped</h2>
            <p className="text-sm text-gray-500 mb-4">
              {data.published_this_week} this week · {data.published_all_time} all time
            </p>
            {data.top_pages.length === 0 ? (
              <p className="text-gray-400 text-sm">No pages published yet.</p>
            ) : (
              <ul className="space-y-2">
                {data.top_pages.map((p, i) => (
                  <li key={i} className="text-sm">
                    <a
                      href={p.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-indigo-600 hover:underline font-medium truncate block"
                      title={p.url}
                    >
                      {p.keyword}
                    </a>
                    <span className="text-gray-400 text-xs">
                      {new Date(p.published_at).toLocaleDateString()}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </div>

          {/* Rank Wins */}
          <div className="bg-white rounded-xl shadow p-5">
            <h2 className="text-lg font-semibold text-gray-800 mb-1">Rank Wins</h2>
            <p className="text-sm text-gray-500 mb-4">Keywords moving up this week</p>
            {data.rank_wins.length === 0 ? (
              <p className="text-gray-400 text-sm">Rank tracking warming up...</p>
            ) : (
              <ul className="space-y-2">
                {data.rank_wins.map((r, i) => (
                  <li key={i} className="flex items-center justify-between text-sm">
                    <span className="text-gray-700 font-medium truncate mr-2">{r.keyword}</span>
                    <span className="flex items-center gap-1 text-green-600 font-semibold whitespace-nowrap">
                      <span className="text-gray-400 line-through text-xs">#{r.from}</span>
                      <span>↑</span>
                      <span>#{r.to}</span>
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </div>

          {/* Indexing Status */}
          <div className="bg-white rounded-xl shadow p-5">
            <h2 className="text-lg font-semibold text-gray-800 mb-1">Indexing Status</h2>
            <p className="text-sm text-gray-500 mb-4">Google visibility</p>
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-sm text-gray-600">Indexed</span>
                <span className="text-sm font-semibold text-green-600">{data.indexing.indexed}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-gray-600">Pending</span>
                <span className="text-sm font-semibold text-yellow-500">{data.indexing.pending}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-gray-600">Failed</span>
                <span className="text-sm font-semibold text-red-500">{data.indexing.failed}</span>
              </div>
            </div>
          </div>

        </div>
      </div>
    </div>
  );
}
