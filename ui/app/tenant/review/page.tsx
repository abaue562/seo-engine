"use client";
import { useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";

interface ReviewItem {
  url: string;
  keyword: string;
  published_at: string;
  status: string;
  failure_reason?: string;
  missing?: string[];
}

interface ReviewQueue {
  business_id: string;
  total: number;
  groups: Record<string, ReviewItem[]>;
  items: ReviewItem[];
}

const REASON_LABELS: Record<string, string> = {
  missing_content: "Content could not be generated",
  wp_error: "WordPress publish failed",
  token_limit: "Content exceeded length limit",
  gsc_error: "Search Console submission error",
  unknown: "Unknown issue",
};

function reasonLabel(r: string): string {
  return REASON_LABELS[r] || r.replace(/_/g, " ");
}

export default function ReviewQueuePage() {
  const params = useSearchParams();
  const businessId = params.get("id") || "";
  const [data, setData] = useState<ReviewQueue | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [actions, setActions] = useState<Record<string, string>>({});

  useEffect(() => {
    if (!businessId) {
      setLoading(false);
      setError("No business ID provided in URL (?id=...)");
      return;
    }
    fetch(`/seo/api/tenant/review-queue?business_id=${encodeURIComponent(businessId)}`)
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
    setActions((prev) => ({ ...prev, [url]: action }));
    // In a real implementation, these would call API endpoints
    console.log(`Action: ${action} for ${url}`);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="text-gray-500 text-lg animate-pulse">Loading review queue...</div>
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

  if (!data || data.total === 0) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-50">
        <div className="text-center">
          <div className="text-4xl mb-4">✅</div>
          <h2 className="text-2xl font-bold text-gray-700 mb-2">All clear</h2>
          <p className="text-gray-500">No pages need review right now.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-5xl mx-auto space-y-6">

        {/* Header */}
        <div className="bg-white rounded-xl shadow p-6">
          <h1 className="text-2xl font-bold text-gray-800 mb-1">Content Review Queue</h1>
          <p className="text-gray-500">
            <span className="font-semibold text-orange-600">{data.total} pages</span> need your attention
          </p>
        </div>

        {/* Groups */}
        {Object.entries(data.groups).map(([reason, items]) => (
          <div key={reason} className="bg-white rounded-xl shadow overflow-hidden">
            <div className="bg-orange-50 border-b border-orange-100 px-6 py-3">
              <h2 className="font-semibold text-orange-800">
                {reasonLabel(reason)}{" "}
                <span className="text-orange-500 font-normal text-sm">({items.length} pages)</span>
              </h2>
            </div>
            <div className="divide-y divide-gray-100">
              {items.map((item, i) => {
                const actionDone = actions[item.url];
                return (
                  <div key={i} className="px-6 py-4">
                    <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-3">
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium text-gray-700 truncate" title={item.url}>
                          {item.keyword}
                        </p>
                        <a
                          href={item.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-xs text-indigo-500 hover:underline truncate block"
                          title={item.url}
                        >
                          {item.url}
                        </a>
                        <p className="text-xs text-gray-400 mt-1">
                          Issue: {reasonLabel(item.failure_reason || reason)}
                          {item.missing && item.missing.length > 0 && (
                            <span className="ml-2 text-red-400">
                              Missing: {item.missing.join(", ")}
                            </span>
                          )}
                        </p>
                      </div>
                      <div className="flex gap-2 flex-shrink-0">
                        {actionDone ? (
                          <span className="text-sm text-green-600 font-medium px-3 py-1.5 bg-green-50 rounded-lg">
                            {actionDone === "regenerate" && "Queued for regeneration"}
                            {actionDone === "edit" && "Opened for editing"}
                            {actionDone === "approve" && "Approved"}
                          </span>
                        ) : (
                          <>
                            <button
                              onClick={() => handleAction(item.url, "regenerate")}
                              className="text-sm px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 transition-colors"
                            >
                              Regenerate
                            </button>
                            <button
                              onClick={() => handleAction(item.url, "edit")}
                              className="text-sm px-3 py-1.5 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors"
                            >
                              Edit
                            </button>
                            <button
                              onClick={() => handleAction(item.url, "approve")}
                              className="text-sm px-3 py-1.5 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
                            >
                              Approve Anyway
                            </button>
                          </>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
