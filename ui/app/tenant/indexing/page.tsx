"use client";
import { useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";

interface IndexPage {
  url: string;
  keyword: string;
  published_at: string;
  days_since_publish: number | null;
  indexing_status: string;
  submitted_at: string | null;
  action: "submit" | "resubmit" | null;
}

interface IndexingSummary {
  total: number;
  indexed: number;
  pct_indexed: number;
  needs_action: number;
}

interface IndexingData {
  business_id: string;
  summary: IndexingSummary;
  pages: IndexPage[];
}

function StatusBadge({ status }: { status: string }) {
  if (status === "submitted") {
    return (
      <span className="inline-block bg-green-100 text-green-700 px-2 py-0.5 rounded-full text-xs font-medium">
        Indexed
      </span>
    );
  }
  if (status === "pending") {
    return (
      <span className="inline-block bg-yellow-100 text-yellow-700 px-2 py-0.5 rounded-full text-xs font-medium">
        Pending
      </span>
    );
  }
  if (status === "failed") {
    return (
      <span className="inline-block bg-red-100 text-red-700 px-2 py-0.5 rounded-full text-xs font-medium">
        Failed
      </span>
    );
  }
  return (
    <span className="inline-block bg-gray-100 text-gray-600 px-2 py-0.5 rounded-full text-xs font-medium">
      Not Submitted
    </span>
  );
}

function truncateUrl(url: string, max = 55): string {
  if (url.length <= max) return url;
  return "..." + url.slice(-(max - 3));
}

export default function IndexingPage() {
  const params = useSearchParams();
  const businessId = params.get("id") || "";
  const [data, setData] = useState<IndexingData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [actionsSent, setActionsSent] = useState<Set<string>>(new Set());

  useEffect(() => {
    if (!businessId) {
      setLoading(false);
      setError("No business ID provided in URL (?id=...)");
      return;
    }
    fetch(`/seo/api/tenant/indexing?business_id=${encodeURIComponent(businessId)}`)
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

  const handleAction = (url: string, action: string) => {
    setActionsSent((prev) => new Set([...prev, url]));
    console.log(`Indexing action: ${action} for ${url}`);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="text-gray-500 text-lg animate-pulse">Loading indexing status...</div>
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

  if (!data || data.summary.total === 0) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="text-center">
          <div className="text-4xl mb-4">🔍</div>
          <h2 className="text-2xl font-bold text-gray-700 mb-2">No published pages yet</h2>
          <p className="text-gray-500">Indexing data will appear once content is live.</p>
        </div>
      </div>
    );
  }

  const { summary, pages } = data;
  const pctColor =
    summary.pct_indexed >= 80
      ? "text-green-600"
      : summary.pct_indexed >= 50
      ? "text-yellow-600"
      : "text-red-600";

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-5xl mx-auto space-y-6">

        {/* Header + Summary Bar */}
        <div className="bg-white rounded-xl shadow p-6">
          <h1 className="text-2xl font-bold text-gray-800 mb-4">Indexing Status</h1>
          <div className="flex flex-col md:flex-row gap-4 md:items-center">
            <div className="flex-1">
              <p className={`text-3xl font-bold ${pctColor}`}>{summary.pct_indexed}%</p>
              <p className="text-gray-500 text-sm mt-0.5">
                of your {summary.total} published pages are indexed
              </p>
            </div>
            <div className="flex gap-6">
              <div className="text-center">
                <p className="text-2xl font-bold text-green-600">{summary.indexed}</p>
                <p className="text-xs text-gray-500">Indexed</p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-bold text-yellow-500">
                  {summary.total - summary.indexed - summary.needs_action}
                </p>
                <p className="text-xs text-gray-500">Pending</p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-bold text-red-500">{summary.needs_action}</p>
                <p className="text-xs text-gray-500">Need Action</p>
              </div>
            </div>
          </div>
          {/* Progress bar */}
          <div className="mt-4 bg-gray-100 rounded-full h-2.5">
            <div
              className="h-2.5 rounded-full bg-green-500 transition-all"
              style={{ width: `${summary.pct_indexed}%` }}
            />
          </div>
        </div>

        {/* Table */}
        <div className="bg-white rounded-xl shadow overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="text-left px-6 py-3 font-semibold text-gray-600">URL</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Keyword</th>
                  <th className="text-center px-4 py-3 font-semibold text-gray-600">Age</th>
                  <th className="text-center px-4 py-3 font-semibold text-gray-600">Status</th>
                  <th className="text-center px-4 py-3 font-semibold text-gray-600">Action</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {pages.map((page, i) => (
                  <tr key={i} className="hover:bg-gray-50 transition-colors">
                    <td className="px-6 py-3">
                      <a
                        href={page.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-indigo-600 hover:underline font-mono text-xs"
                        title={page.url}
                      >
                        {truncateUrl(page.url)}
                      </a>
                    </td>
                    <td className="px-4 py-3 text-gray-700 max-w-[160px] truncate" title={page.keyword}>
                      {page.keyword}
                    </td>
                    <td className="px-4 py-3 text-center text-gray-500">
                      {page.days_since_publish !== null ? `${page.days_since_publish}d` : "—"}
                    </td>
                    <td className="px-4 py-3 text-center">
                      <StatusBadge status={page.indexing_status} />
                    </td>
                    <td className="px-4 py-3 text-center">
                      {page.action && !actionsSent.has(page.url) ? (
                        <button
                          onClick={() => handleAction(page.url, page.action!)}
                          className={`text-xs px-3 py-1.5 rounded-lg font-medium transition-colors ${
                            page.action === "resubmit"
                              ? "bg-red-100 text-red-700 hover:bg-red-200"
                              : "bg-indigo-100 text-indigo-700 hover:bg-indigo-200"
                          }`}
                        >
                          {page.action === "resubmit" ? "Resubmit" : "Submit"}
                        </button>
                      ) : actionsSent.has(page.url) ? (
                        <span className="text-xs text-green-600 font-medium">Queued</span>
                      ) : (
                        <span className="text-gray-300 text-xs">—</span>
                      )}
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
