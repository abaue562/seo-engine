"use client";

import type { FullPowerReport } from "@/lib/api";

function Section({ title, badge, color, children }: { title: string; badge: string; color: string; children: React.ReactNode }) {
  return (
    <div className="card" style={{ borderLeft: `3px solid ${color}` }}>
      <div className="flex items-center gap-2 mb-3">
        <span className="badge" style={{ background: `${color}20`, color }}>{badge}</span>
        <h3 className="font-bold text-sm">{title}</h3>
      </div>
      {children}
    </div>
  );
}

function Item({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="flex justify-between text-xs py-1" style={{ borderBottom: "1px solid var(--border)" }}>
      <span style={{ color: "var(--muted)" }}>{label}</span>
      <span className="font-semibold">{value}</span>
    </div>
  );
}

export default function EdgeTools({ report }: { report: FullPowerReport | null }) {
  if (!report) return null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const rpt = report as any;
  const has = (arr: unknown) => Array.isArray(arr) && arr.length > 0;

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-bold">Edge Tools Active</h2>
        <span className="text-xs" style={{ color: "var(--muted)" }}>
          {rpt.tools_used.length} tools | {rpt.run_time_seconds.toFixed(1)}s
        </span>
      </div>

      {/* Signal Burst */}
      {has(rpt.signal_burst_plans) && (
        <Section title="Signal Burst" badge="BURST" color="var(--red)">
          {rpt.signal_burst_plans.map((b: any, i: number) => (
            <div key={i} className="mb-2">
              <div className="text-sm font-semibold">{b.keyword} (#{b.position})</div>
              <div className="text-xs" style={{ color: "var(--muted)" }}>
                Intensity: <span style={{ color: "var(--yellow)" }}>{b.intensity?.toUpperCase()}</span>
              </div>
              <div className="text-xs" style={{ color: "var(--muted)" }}>{b.recommendation}</div>
            </div>
          ))}
        </Section>
      )}

      {/* CTR Opportunities */}
      {has(rpt.ctr_opportunities) && (
        <Section title="CTR Optimization" badge="CTR" color="var(--yellow)">
          {rpt.ctr_opportunities.map((c: any, i: number) => (
            <div key={i} className="mb-2">
              <div className="text-sm font-semibold">{c.keyword}</div>
              <div className="flex gap-3 text-xs" style={{ color: "var(--muted)" }}>
                <span>Position: #{c.position}</span>
                <span>CTR: {(c.ctr * 100).toFixed(1)}%</span>
                <span>Potential: +{c.potential_clicks} clicks</span>
              </div>
            </div>
          ))}
        </Section>
      )}

      {/* SERP Hijack */}
      {has(rpt.serp_clusters) && (
        <Section title="SERP Hijack Cluster" badge="SERP" color="var(--accent)">
          {rpt.serp_clusters.map((c: any, i: number) => (
            <div key={i}>
              <div className="text-sm font-semibold">{c.keyword} (#{c.position})</div>
              <div className="text-xs" style={{ color: "var(--muted)" }}>
                {c.total_pages} pages planned | {c.supporting_count} supporting | {c.link_count} internal links
              </div>
              {c.main_page && (
                <div className="text-xs mt-1" style={{ color: "var(--green)" }}>
                  Main: {c.main_page.title}
                </div>
              )}
            </div>
          ))}
        </Section>
      )}

      {/* Rapid Updates */}
      {has(rpt.rapid_updates) && (
        <Section title="Rapid Updates" badge="FRESH" color="var(--green)">
          {rpt.rapid_updates.map((r: any, i: number) => (
            <div key={i}>
              <div className="text-sm font-semibold">{r.keyword} (#{r.position})</div>
              <div className="text-xs" style={{ color: "var(--muted)" }}>
                {r.updates?.length} updates | +{r.total_words} words
              </div>
              {r.updates?.slice(0, 2).map((u: any, j: number) => (
                <div key={j} className="text-xs mt-1" style={{ color: "var(--muted)" }}>
                  [{u.type}] {u.instruction?.slice(0, 80)}...
                </div>
              ))}
            </div>
          ))}
        </Section>
      )}

      {/* Competitor Threats */}
      {has(rpt.competitor_threats) && (
        <Section title="Competitor Threats" badge="THREAT" color="var(--red)">
          {rpt.competitor_threats.map((t: any, i: number) => (
            <div key={i} className="mb-2">
              <div className="text-sm font-semibold">{t.competitor}</div>
              <div className="text-xs" style={{ color: "var(--muted)" }}>
                {t.keyword} — {t.detail?.slice(0, 100)}
              </div>
              <div className="text-xs" style={{ color: t.threat_level === "critical" ? "var(--red)" : "var(--yellow)" }}>
                Threat: {t.threat_level?.toUpperCase()}
              </div>
            </div>
          ))}
        </Section>
      )}

      {/* Authority Gap */}
      {has(rpt.authority_gaps) && (
        <Section title="Authority Gap" badge="AUTH" color="#a78bfa">
          {rpt.authority_gaps.map((g: any, i: number) => (
            <div key={i} className="mb-2">
              <div className="text-sm font-semibold">{g.keyword}</div>
              <Item label="DA Gap" value={`${g.gap} points`} />
              <Item label="Severity" value={g.severity} />
              <div className="text-xs mt-1" style={{ color: "var(--muted)" }}>{g.recommendation}</div>
            </div>
          ))}
        </Section>
      )}

      {/* Suppression */}
      {has(rpt.suppression_actions) && (
        <Section title="Competitive Suppression" badge="SUPPRESS" color="var(--yellow)">
          {rpt.suppression_actions.slice(0, 3).map((s: any, i: number) => (
            <div key={i} className="mb-2">
              <div className="text-xs">
                <span className="font-semibold">{s.action}</span> vs {s.competitor}
              </div>
              <div className="text-xs" style={{ color: "var(--muted)" }}>
                {s.keyword} (us #{s.our_rank} vs them #{s.their_rank})
              </div>
            </div>
          ))}
        </Section>
      )}

      {/* Pressure Campaign */}
      {has(rpt.pressure_campaigns) && (
        <Section title="Pressure Campaign" badge="PRESSURE" color="var(--accent)">
          {rpt.pressure_campaigns.map((p: any, i: number) => (
            <div key={i}>
              <div className="text-sm font-semibold">{p.keyword}</div>
              <div className="text-xs" style={{ color: "var(--muted)" }}>
                Total assets: {p.total_assets}
              </div>
              {p.assets && Object.entries(p.assets).map(([k, v]) => (
                <Item key={k} label={k} value={String(v)} />
              ))}
            </div>
          ))}
        </Section>
      )}

      {/* Traffic Generation — Content Bundles */}
      {has(rpt.content_bundles) && (
        <Section title="Traffic Generator" badge="TRAFFIC" color="var(--green)">
          {rpt.content_bundles.map((b: any, i: number) => (
            <div key={i}>
              <div className="text-sm font-semibold mb-2">{b.keyword} ({b.formats} formats)</div>
              {b.tiktok_script?.hook && (
                <div className="p-2 rounded mb-2" style={{ background: "var(--bg)" }}>
                  <div className="text-xs font-semibold" style={{ color: "var(--red)" }}>TikTok Script</div>
                  <div className="text-xs mt-1">Hook: {b.tiktok_script.hook}</div>
                  <div className="text-xs">Body: {b.tiktok_script.body?.slice(0, 100)}...</div>
                  <div className="text-xs">CTA: {b.tiktok_script.cta}</div>
                </div>
              )}
              {b.gbp_post?.text && (
                <div className="p-2 rounded mb-2" style={{ background: "var(--bg)" }}>
                  <div className="text-xs font-semibold" style={{ color: "var(--accent)" }}>GBP Post</div>
                  <div className="text-xs mt-1">{b.gbp_post.text?.slice(0, 150)}...</div>
                </div>
              )}
              {b.social_post?.text && (
                <div className="p-2 rounded mb-2" style={{ background: "var(--bg)" }}>
                  <div className="text-xs font-semibold" style={{ color: "var(--yellow)" }}>Social Post</div>
                  <div className="text-xs mt-1">{b.social_post.text}</div>
                </div>
              )}
              {b.blog_article?.title && (
                <div className="text-xs" style={{ color: "var(--muted)" }}>
                  Blog: {b.blog_article.title}
                </div>
              )}
            </div>
          ))}
        </Section>
      )}

      {/* Demand Generation */}
      {has(rpt.demand_campaigns) && (
        <Section title="Demand Generation" badge="DEMAND" color="#f59e0b">
          {rpt.demand_campaigns.map((d: any, i: number) => (
            <div key={i}>
              <div className="text-sm font-semibold mb-1">Target search: "{d.target_search}"</div>
              <div className="text-xs mb-2" style={{ color: "var(--muted)" }}>
                Expected: {d.expected_searches} branded searches/mo
              </div>
              <div className="text-xs font-semibold mb-1">Content hooks:</div>
              {d.hooks?.map((h: string, j: number) => (
                <div key={j} className="text-xs py-1" style={{ color: "var(--muted)", borderBottom: "1px solid var(--border)" }}>
                  {j + 1}. {h}
                </div>
              ))}
            </div>
          ))}
        </Section>
      )}

      {/* CTR Variants */}
      {has(rpt.ctr_variants) && (
        <Section title="CTR Title Variants" badge="CTR TEST" color="var(--yellow)">
          {rpt.ctr_variants.map((v: any, i: number) => (
            <div key={i} className="p-2 rounded mb-2" style={{ background: "var(--bg)" }}>
              <div className="text-xs font-semibold" style={{ color: "var(--accent)" }}>{v.style?.toUpperCase()}</div>
              <div className="text-sm font-semibold mt-1">{v.title}</div>
              <div className="text-xs mt-1" style={{ color: "var(--muted)" }}>{v.meta_description}</div>
              <div className="text-xs mt-1" style={{ color: "var(--green)" }}>Predicted: {v.predicted_ctr_boost}</div>
            </div>
          ))}
        </Section>
      )}

      {/* Authority Swarm — Backlink Engine */}
      {has(rpt.authority_swarm) && (
        <Section title="Backlink Swarm" badge="LINKS" color="var(--red)">
          {rpt.authority_swarm.map((s: any, i: number) => (
            <div key={i}>
              <div className="text-sm font-semibold">{s.keyword}</div>
              <Item label="Content nodes" value={s.total_nodes} />
              <Item label="Velocity" value={s.velocity} />
              <Item label="Est. days" value={s.estimated_days} />
              {s.anchor_mix && (
                <div className="mt-1">
                  <div className="text-xs font-semibold" style={{ color: "var(--muted)" }}>Anchor mix:</div>
                  {Object.entries(s.anchor_mix).map(([k, v]) => (
                    <Item key={k} label={k} value={String(v)} />
                  ))}
                </div>
              )}
              {s.nodes?.slice(0, 3).map((n: any, j: number) => (
                <div key={j} className="p-2 rounded mt-2" style={{ background: "var(--bg)" }}>
                  <div className="text-xs"><span className="font-semibold">[{n.type}]</span> {n.platform}</div>
                  <div className="text-xs" style={{ color: "var(--muted)" }}>Anchor: "{n.anchor}"</div>
                  {n.content_preview && <div className="text-xs mt-1" style={{ color: "var(--muted)" }}>{n.content_preview}...</div>}
                </div>
              ))}
            </div>
          ))}
        </Section>
      )}

      {/* Market Domination */}
      {rpt.market_domination?.total_keywords > 0 && (
        <Section title="Market Domination" badge="DOMINATE" color="var(--accent)">
          <Item label="Total keywords" value={rpt.market_domination.total_keywords} />
          <Item label="Pages to create" value={rpt.market_domination.pages_to_create} />
          <Item label="Internal links" value={rpt.market_domination.link_count} />
          <Item label="Coverage" value={`${rpt.market_domination.coverage_pct}%`} />
          {rpt.market_domination.content_plan?.slice(0, 4).map((c: any, i: number) => (
            <div key={i} className="text-xs py-1" style={{ borderBottom: "1px solid var(--border)" }}>
              <span className="font-semibold">[{c.type}]</span> {c.title || c.keyword}
              <span className="ml-1" style={{ color: c.priority === "high" ? "var(--red)" : "var(--muted)" }}>
                ({c.priority})
              </span>
            </div>
          ))}
        </Section>
      )}

      {/* AI Visibility */}
      {rpt.ai_visibility?.composite != null && (
        <Section title="AI Visibility Score" badge="AI" color="#a78bfa">
          <Item label="Composite" value={`${rpt.ai_visibility.composite}/10`} />
          <Item label="Answer Readiness" value={`${rpt.ai_visibility.answer_readiness}/10`} />
          <Item label="Entity Saturation" value={`${rpt.ai_visibility.entity_saturation}/10`} />
          <Item label="Mention Density" value={`${rpt.ai_visibility.mention_density}/10`} />
          <Item label="Content Authority" value={`${rpt.ai_visibility.content_authority}/10`} />
          <div className="text-xs mt-2" style={{ color: "var(--muted)" }}>
            {rpt.ai_visibility.status}
          </div>
        </Section>
      )}

      {/* Tools summary */}
      <div className="text-xs" style={{ color: "var(--muted)" }}>
        {rpt.tools_used.length} tools activated: {rpt.tools_used.join(", ")}
      </div>
    </div>
  );
}
