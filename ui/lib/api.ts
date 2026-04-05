/**
 * API client — talks directly to the Python backend.
 * Uses direct URL to avoid Next.js proxy timeout issues.
 */

const BASE =
  typeof window !== "undefined"
    ? "/seo/api"
    : process.env.SEO_API_INTERNAL || "http://seo-engine-api:8900";

async function request<T>(path: string, body?: unknown): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 300_000); // 5 min timeout

  try {
    const res = await fetch(`${BASE}${path}`, {
      method: body ? "POST" : "GET",
      headers: body ? { "Content-Type": "application/json" } : {},
      body: body ? JSON.stringify(body) : undefined,
      signal: controller.signal,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`API ${path}: ${res.status}${text ? ` — ${text.slice(0, 200)}` : ""}`);
    }
    return res.json();
  } finally {
    clearTimeout(timeout);
  }
}

// --- Types ---

export interface Task {
  action: string;
  target: string;
  why: string;
  impact: "high" | "medium" | "low";
  type: "GBP" | "WEBSITE" | "CONTENT" | "AUTHORITY";
  role: "primary" | "supporting" | "experimental";
  execution_mode: "AUTO" | "MANUAL" | "ASSISTED";
  estimated_result: string;
  time_to_result: string;
  execution: string;
  impact_score: number;
  ease_score: number;
  speed_score: number;
  confidence_score: number;
  total_score: number;
  priority_rank: number;
}

export interface TaskBatch {
  input_type: string;
  tasks: Task[];
  business_name: string;
  run_id: string;
  filtered_count: number;
}

export interface ExecResult {
  task_id: string;
  status: string;
  output: Record<string, unknown>;
}

export interface Pattern {
  pattern: string;
  task_type: string;
  times_used: number;
  success_rate: number;
  avg_performance: number;
  is_killed: boolean;
}

export interface LearningReport {
  cycle_type: string;
  tasks_evaluated: number;
  successful: number;
  failed: number;
  patterns_updated: number;
  top_performers: { pattern: string; success_rate: number; avg_perf: number }[];
  recommendations: string[];
}

export interface FullPowerReport {
  tasks: Task[];
  task_count: number;
  ctr_opportunities: Record<string, unknown>[];
  serp_clusters: Record<string, unknown>[];
  rapid_updates: Record<string, unknown>[];
  competitor_threats: Record<string, unknown>[];
  signal_burst_plans: Record<string, unknown>[];
  authority_gaps: Record<string, unknown>[];
  suppression_actions: Record<string, unknown>[];
  pressure_campaigns: Record<string, unknown>[];
  ai_visibility: Record<string, unknown>;
  tools_used: string[];
  run_time_seconds: number;
  [key: string]: unknown;
}

// --- API calls ---

export const api = {
  health: () => request<{ status: string; claude_mode: string }>("/health"),

  fullPower: (business: Record<string, unknown>, businessId = "default") =>
    request<FullPowerReport>("/full-power", { business, business_id: businessId }),

  analyze: (business: Record<string, unknown>, inputType = "FULL") =>
    request<TaskBatch>("/analyze", { business, input_type: inputType }),

  orchestrate: (business: Record<string, unknown>, inputType = "FULL", disagreementMode = false) =>
    request<{ tasks: TaskBatch; pipeline_log: Record<string, unknown> }>("/orchestrate", {
      business,
      input_type: inputType,
      disagreement_mode: disagreementMode,
    }),

  run: (params: {
    business: Record<string, unknown>;
    business_id: string;
    input_type?: string;
    auto_execute?: boolean;
    shadow_mode?: boolean;
  }) => request<{
    tasks: TaskBatch;
    execution_results: ExecResult[];
    events: Record<string, unknown>[];
    freshness: string;
  }>("/run", params),

  execute: (tasks: Task[], business: Record<string, unknown>, businessId: string, shadowMode = false) =>
    request<{ results: ExecResult[]; executed: number; queued: number; skipped: number; failed: number }>("/execute", {
      tasks,
      business,
      business_id: businessId,
      shadow_mode: shadowMode,
    }),

  approve: (taskId: string, businessId: string) =>
    request<ExecResult>("/approve", { task_id: taskId, business_id: businessId }),

  learn: (businessId: string, cycle: "weekly" | "monthly" = "weekly") =>
    request<LearningReport>("/learn", { business_id: businessId, cycle }),

  patterns: () => request<Pattern[]>("/patterns"),
};
