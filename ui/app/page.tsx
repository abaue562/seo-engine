"use client";

import { useState, useCallback } from "react";
import BusinessInput, { type BusinessData } from "@/components/BusinessInput";
import TaskDetail from "@/components/TaskDetail";
import CompoundImpact from "@/components/CompoundImpact";
import SystemStatus from "@/components/SystemStatus";
import ActivityFeed from "@/components/ActivityFeed";
import EdgeTools from "@/components/EdgeTools";
import { api, type Task, type ExecResult, type FullPowerReport } from "@/lib/api";

type Mode = "analyze" | "orchestrate" | "full-power";
type OpMode = "manual" | "assisted" | "autonomous";

export default function Dashboard() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [business, setBusiness] = useState<BusinessData | null>(null);
  const [businessId, setBusinessId] = useState("biz-001");
  const [loading, setLoading] = useState(false);
  const [executingId, setExecutingId] = useState<number | null>(null);
  const [execResults, setExecResults] = useState<Record<number, ExecResult>>({});
  const [fullReport, setFullReport] = useState<FullPowerReport | null>(null);
  const [mode, setMode] = useState<Mode>("analyze");
  const [opMode, setOpMode] = useState<OpMode>("assisted");
  const [activity, setActivity] = useState<
    { text: string; type: "execution" | "event" | "learning"; timestamp: string }[]
  >([]);
  const [error, setError] = useState<string | null>(null);
  const [runStats, setRunStats] = useState<{
    tasks: number;
    filtered: number;
    run_id: string;
    time_ms: number;
  } | null>(null);

  const log = useCallback(
    (text: string, type: "execution" | "event" | "learning" = "event") => {
      setActivity((prev) => [
        { text, type, timestamp: new Date().toLocaleTimeString() },
        ...prev.slice(0, 29),
      ]);
    },
    []
  );

  const runSystem = async (biz: BusinessData) => {
    setBusiness(biz);
    setLoading(true);
    setError(null);
    setExecResults({});
    setRunStats(null);
    setFullReport(null);

    const id = biz.business_name.toLowerCase().replace(/\s+/g, "-").slice(0, 30);
    setBusinessId(id);

    log(`Running ${mode} for ${biz.business_name}...`);
    const start = Date.now();

    try {
      let result;
      if (mode === "full-power") {
        log("Full Power mode — running all edge tools...", "event");
        const fp = await api.fullPower(biz, id);
        setFullReport(fp);
        // Convert full power tasks to TaskBatch format
        result = { tasks: fp.tasks || [], filtered_count: 0, run_id: "full-power", business_name: biz.business_name, input_type: "FULL" };
        log(`Edge tools activated: ${fp.tools_used.length} (${fp.tools_used.join(", ")})`, "event");
      } else if (mode === "analyze") {
        result = await api.analyze(biz);
      } else {
        const orchResult = await api.orchestrate(biz);
        result = orchResult.tasks;
      }

      const elapsed = Date.now() - start;
      setTasks(result.tasks);
      setRunStats({
        tasks: result.tasks.length,
        filtered: result.filtered_count,
        run_id: result.run_id,
        time_ms: elapsed,
      });

      log(
        `Done: ${result.tasks.length} tasks in ${(elapsed / 1000).toFixed(1)}s (${result.filtered_count} filtered)`
      );

      // Autonomous mode: auto-execute AUTO tasks
      if (opMode === "autonomous" && result.tasks.length > 0) {
        const autoTasks = result.tasks.filter((t: Task) => t.execution_mode === "AUTO");
        if (autoTasks.length > 0) {
          log(`Autonomous: executing ${autoTasks.length} AUTO tasks...`, "execution");
          try {
            const execRes = await api.execute(autoTasks, biz, id);
            const newResults: Record<number, ExecResult> = {};
            let ri = 0;
            result.tasks.forEach((t: Task, i: number) => {
              if (t.execution_mode === "AUTO" && execRes.results[ri]) {
                newResults[i] = execRes.results[ri];
                ri++;
              }
            });
            setExecResults(newResults);
            log(`Autonomous: ${execRes.executed} executed, ${execRes.queued} queued`, "execution");
          } catch (err: any) {
            log(`Autonomous execution failed: ${err?.message || err}`, "execution");
          }
        }
      }
    } catch (err: any) {
      const msg = err?.message || String(err);
      setError(msg);
      log(`Failed: ${msg}`);
    }

    setLoading(false);
  };

  const executeTask = async (task: Task, index: number) => {
    if (!business) return;
    setExecutingId(index);
    log(`Executing: ${task.action}`, "execution");

    try {
      const result = await api.execute([task], business, businessId);
      if (result.results?.length > 0) {
        setExecResults((prev) => ({ ...prev, [index]: result.results[0] }));
        log(
          `Result: ${result.executed} executed, ${result.queued} queued, ${result.skipped} skipped`,
          "execution"
        );
      }
    } catch (err: any) {
      log(`Execution failed: ${err?.message || err}`, "execution");
    }

    setExecutingId(null);
  };

  const executeAllAuto = async () => {
    if (!business || !tasks.length) return;
    const autoTasks = tasks.filter((t) => t.execution_mode === "AUTO");
    if (!autoTasks.length) {
      log("No AUTO tasks to execute", "execution");
      return;
    }
    log(`Executing ${autoTasks.length} AUTO tasks...`, "execution");
    try {
      const result = await api.execute(autoTasks, business, businessId);
      const newResults: Record<number, ExecResult> = { ...execResults };
      let ri = 0;
      tasks.forEach((t, i) => {
        if (t.execution_mode === "AUTO" && result.results[ri]) {
          newResults[i] = result.results[ri];
          ri++;
        }
      });
      setExecResults(newResults);
      log(`Batch: ${result.executed} executed, ${result.queued} queued, ${result.failed} failed`, "execution");
    } catch (err: any) {
      log(`Batch failed: ${err?.message || err}`, "execution");
    }
  };

  const executedCount = Object.values(execResults).filter((r) => r.status === "success").length;

  return (
    <div className="min-h-screen p-6 max-w-[1400px] mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-2">
        <div>
          <h1 className="text-2xl font-bold">SEO Engine</h1>
          <p className="text-sm" style={{ color: "var(--muted)" }}>
            {business ? `${business.business_name} — ${business.primary_city}` : "Enter a business to start"}
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Analysis mode */}
          <div className="flex rounded-lg overflow-hidden" style={{ border: "1px solid var(--border)" }}>
            <button
              className="px-3 py-1 text-sm"
              style={{
                background: mode === "analyze" ? "var(--accent)" : "transparent",
                color: mode === "analyze" ? "white" : "var(--muted)",
              }}
              onClick={() => setMode("analyze")}
            >
              Fast (1 call)
            </button>
            <button
              className="px-3 py-1 text-sm"
              style={{
                background: mode === "orchestrate" ? "var(--accent)" : "transparent",
                color: mode === "orchestrate" ? "white" : "var(--muted)",
              }}
              onClick={() => setMode("orchestrate")}
            >
              Deep (4 agents)
            </button>
            <button
              className="px-3 py-1 text-sm"
              style={{
                background: mode === "full-power" ? "var(--red)" : "transparent",
                color: mode === "full-power" ? "white" : "var(--muted)",
              }}
              onClick={() => setMode("full-power")}
            >
              Full Power
            </button>
          </div>

          {/* Operation mode */}
          <div className="flex rounded-lg overflow-hidden" style={{ border: "1px solid var(--border)" }}>
            {(["manual", "assisted", "autonomous"] as OpMode[]).map((m) => (
              <button
                key={m}
                className="px-3 py-1 text-xs capitalize"
                style={{
                  background: opMode === m ? (m === "autonomous" ? "var(--green)" : "var(--accent)") : "transparent",
                  color: opMode === m ? "white" : "var(--muted)",
                }}
                onClick={() => setOpMode(m)}
              >
                {m}
              </button>
            ))}
          </div>

          {tasks.length > 0 && (
            <button className="btn btn-ghost" onClick={executeAllAuto}>
              Execute All AUTO
            </button>
          )}
        </div>
      </div>

      {/* System status bar */}
      <div className="flex items-center justify-between mb-6">
        <SystemStatus />
        {executedCount > 0 && (
          <span className="text-xs" style={{ color: "var(--green)" }}>
            {executedCount} task{executedCount !== 1 ? "s" : ""} executed this session
          </span>
        )}
      </div>

      {/* Two-column layout */}
      <div className="grid grid-cols-[1fr_380px] gap-6">
        {/* Left: Input + Impact + Tasks */}
        <div className="space-y-4">
          <BusinessInput onSubmit={runSystem} loading={loading} />

          {/* Run stats */}
          {runStats && (
            <div className="flex gap-4 text-xs" style={{ color: "var(--muted)" }}>
              <span>Run: {runStats.run_id}</span>
              <span>Tasks: {runStats.tasks}</span>
              <span>Filtered: {runStats.filtered}</span>
              <span>Time: {(runStats.time_ms / 1000).toFixed(1)}s</span>
              <span>Mode: {mode} / {opMode}</span>
            </div>
          )}

          {error && (
            <div className="card" style={{ borderLeft: "3px solid var(--red)" }}>
              <p className="text-sm font-semibold" style={{ color: "var(--red)" }}>Error</p>
              <p className="text-sm mt-1" style={{ color: "var(--muted)" }}>{error}</p>
              <p className="text-xs mt-2" style={{ color: "var(--muted)" }}>
                Make sure the backend is running: <code>python -m api.server</code>
              </p>
            </div>
          )}

          {/* Compound impact */}
          <CompoundImpact tasks={tasks} />

          {/* Task list */}
          {tasks.length > 0 && (
            <div className="space-y-3">
              <h2 className="text-lg font-bold">
                Top {tasks.length} Actions
                <span className="text-sm font-normal ml-2" style={{ color: "var(--muted)" }}>
                  ranked by ROI score
                </span>
              </h2>
              {tasks.map((task, i) => (
                <TaskDetail
                  key={`${task.priority_rank}-${i}`}
                  task={task}
                  allTasks={tasks}
                  execResult={execResults[i]}
                  onExecute={(t) => executeTask(t, i)}
                  executing={executingId === i}
                />
              ))}
            </div>
          )}

          {!loading && tasks.length === 0 && !error && (
            <div className="card text-center py-12">
              <p className="text-lg font-bold" style={{ color: "var(--muted)" }}>
                No tasks yet
              </p>
              <p className="text-sm mt-2" style={{ color: "var(--muted)" }}>
                Enter your business details and click "Run SEO System"
              </p>
            </div>
          )}
        </div>

        {/* Right: Edge Tools + Activity */}
        <div className="space-y-4">
          <EdgeTools report={fullReport} />
          <ActivityFeed items={activity} />
        </div>
      </div>
    </div>
  );
}
