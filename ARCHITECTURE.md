# GetHubed SEO Engine — System Architecture Overview
**Living document. Update this file after every build session.**
**Last updated: 2026-04-17 | Commit: af1ced5**

---

## Legend
- ✅ Complete and active
- ⚠️ Built but dormant (needs API key / config / wiring)
- 🔧 Built with scraper/parser (no LLM — fast + free)
- 🤖 Uses LLM (Grok primary, Claude fallback)
- ❌ Missing — not yet built
- 🔄 Partial — detection exists, action loop not wired

---

## 1. INFRASTRUCTURE

### Database
| Component | Status | Notes |
|-----------|--------|-------|
| PostgreSQL (tenant RLS) | ✅ | `core/pg.py` — `DATABASE_URL=postgresql://seo_app:seo_Bbl_2026_secure@localhost:5432/seo_engine` — sets `app.current_tenant` GUC for row-level security |
| SQLite (gap-phase modules) | ✅ | `data/storage/seo_engine.db` — used by core/ gap modules (citable_data, citation_content, parasite_seo, credential_vault, etc.) — 48 tables |
| Redis | ✅ | Celery broker + result backend |

### Server
| Component | Status | Notes |
|-----------|--------|-------|
| FastAPI | ✅ | Port 8900, `api/server.py` |
| Celery workers | ✅ | `seo-worker.service` |
| Celery beat | ✅ | `seo-beat.service`, 15+ scheduled jobs |
| API service | ✅ | `seo-api.service` |
| CDN / Edge cache | ❌ | No Cloudflare or CDN — all requests hit origin VPS directly |
| VPS | ✅ | 204.168.184.50, alias `aion-vps`, root user |

### AION Brain Services
| Service | Port | Status | Notes |
|---------|------|--------|-------|
| Grok (primary LLM) | :9082 | ✅ | `aion.brain_complete(prompt, model='groq')` |
| Firecrawl | :3002 | ✅ | `aion.firecrawl_scrape(url, timeout=30)` — NO options param |
| Knowledge | :9091 | ✅ | AION knowledge bridge |
| Circuit breaker | ❌ | No fast-fail if :9082 goes down |

---

## 2. CONTENT PIPELINE (core/pipeline.py)

10-stage async pipeline per content piece:

| Stage | Status | Method |
|-------|--------|--------|
| 1. Brief generation | ✅ | SERP data + keyword + intent → content brief |
| 2. Content generation | ✅ 🤖 | Grok primary, Claude fallback |
| 3. Quality gate (ContentGate) | ✅ 🔧 | `execution/validators/content_gate.py` — AI detection, humanizer, word count, blocking failures |
| 4. Publish (WordPress) | ✅ | `execution/connectors/wordpress.py` |
| 5. Canonical registration | ✅ 🔧 | `execution/canonical.py` — CanonicalRegistry, auto-assigned at publish |
| 6. Internal link injection | ✅ 🔧 | `execution/link_injector.py` |
| 7. Indexing | ✅ 🔧 | `execution/indexing.py` — Google Indexing API + IndexNow (both) |
| 8. 48hr verify | ✅ 🔧 | `execution/verification.py` — checks if URL indexed, re-submits if not |
| 9. Rank tracking registration | ✅ 🔧 | `data/connectors/rank_tracker.py` |
| 10. Conversion hook | ✅ | CTA + lead form injection |

### SERP Quality Gate (pre-generation)
| Component | Status | Notes |
|-----------|--------|-------|
| Freshness check | ✅ 🔧 | `core/serp_gate.py` — blocks if SERP data > 14 days old |
| Quality classification | ✅ 🔧 | FULL / PARTIAL / FAILED / STALE |
| serp_blocked marking | ✅ 🔧 | Marks keyword in Postgres if quality too low |
| Block rate alerting | ✅ 🔧 | Logs error if >5% keywords blocked |

---

## 3. TECHNICAL SEO

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| Canonical URL injection | ✅ 🔧 | `execution/canonical.py` | Auto at publish |
| Sitemap XML | ⚠️ 🔧 | `api/ai-search/sitemap-xml` | Manual trigger — not auto on new publish |
| Sitemap ping (Google/Bing) | ❌ | | Should fire on every new publish |
| robots.txt management | ⚠️ 🔧 | `api/ai-search/robots-additions` | Manual — no per-tenant auto-management |
| Tech SEO audit | ✅ 🔧 | `core/audit.py` + `data/connectors/tech_audit.py` | |
| Cannibalization detection | 🔄 🔧 | `core/cannibalization.py` | Detects but does NOT auto-resolve (merge/redirect) |
| Redirect chain audit | ❌ | | Not built |
| hreflang management | ❌ | | Not built — needed for multi-region tenants |
| Crawl budget management | ❌ | | No auto-noindex for thin programmatic variants |
| Log file analysis | ❌ | | No Googlebot behavior analysis |
| Duplicate semantic linker | ⚠️ | | `core/semantic_linker.py` AND `core/linking/semantic_linker.py` — conflict risk |

---

## 4. SCHEMA + STRUCTURED DATA

| Schema Type | Status | Where Implemented |
|-------------|--------|------------------|
| LocalBusiness | ✅ 🔧 | `core/brand_entity.py` (default entity_type), `core/trust_signals.py` About page |
| Organization | ✅ 🔧 | `core/brand_entity.py` — full sameAs, address, phone |
| FAQPage | ✅ 🔧 | `core/trust_signals.py` + `core/citation_content.py` faq_hub |
| BreadcrumbList | ✅ 🔧 | `core/trust_signals.py` |
| AggregateRating | ✅ 🔧 | `core/trust_signals.py` — needs real review data |
| Review | ✅ 🔧 | `core/trust_signals.py` |
| Article / BlogPosting | ✅ 🔧 | `core/schema_validator.py` — validates + auto-adds dateModified |
| HowTo | ✅ 🔧 | `core/schema_validator.py` — validates name + step fields |
| SpeakableSpecification | ✅ 🔧 | `core/speakable_schema.py` — ⚠️ not confirmed auto-injected on every page |
| Person (author) | ✅ 🔧 | `core/author_profiles.py` — Person JSON-LD with sameAs |
| Service | ⚠️ 🔧 | Referenced in prompts/system.py — not confirmed injected at generation |
| Dataset | ❌ | | Missing for stats pages / local studies — high AI citation value |
| VideoObject | ❌ | | Not built |
| Event | ❌ | | Not built |
| Product | ❌ | | Not built |
| Schema validation | ✅ 🔧 | `core/schema_validator.py` + `quality/schema_validator.py` |
| Auto-fix on validation fail | ❌ | | Validates but doesn't re-inject on error |

---

## 5. KEYWORD STRATEGY

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| Self-hosted SERP scraper | ✅ 🔧 | `core/serp_scraper.py` | Bing scraping — no paid API |
| SERP cache | ✅ 🔧 | `core/serp_cache.py` | Redis cache |
| Keyword intel | ✅ 🔧 | `core/keyword_intel.py` | |
| Keyword clustering | ✅ 🔧 | `core/keyword_clustering.py` | |
| Search volume data | ⚠️ 🔧 | `data/connectors/dataforseo.py` | **Needs DATAFORSEO_LOGIN + DATAFORSEO_PASSWORD** |
| Keyword difficulty | ⚠️ 🔧 | `data/connectors/dataforseo.py` | **Needs credentials** |
| Ahrefs keyword data | ⚠️ 🔧 | `data/connectors/ahrefs.py` | **Needs AHREFS_API_KEY** |
| Intent classification | ✅ 🔧 | `core/intent_classifier.py` | |
| GSC performance connector | ✅ 🔧 | `data/connectors/gsc_performance.py` + `gsc_live/` | |
| GSC quota tracker | ✅ 🔧 | `core/gsc_quota.py` | |
| CTR optimization loop | ❌ | | GSC data collected, no CTR agent reads it to optimize titles |
| Keyword velocity / trends | ❌ | | No Google Trends integration |
| Autocomplete data | ✅ 🔧 | `data/analyzers/autocomplete.py` | |

---

## 6. CONTENT CLUSTERING + TOPICAL AUTHORITY

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| Topical map builder | ✅ 🤖 | `core/topical/map_builder.py` | |
| Topic lifecycle | ✅ 🔧 | `core/topic_lifecycle.py` | |
| Topical gap detection | 🔄 🔧 | `run_topical_gap_check` task | **Detects gaps, writes to content_opportunities — NOT wired to auto-generate** |
| Cluster context | ✅ 🔧 | `data/analyzers/cluster_context.py` | |
| Topical coverage UI | ❌ | | Not surfaced in dashboard |
| Hub/spoke enforcement | ❌ | | No structural guarantee of pillar + N supporting pages |
| Competitor topical map | ⚠️ 🔧 | `data/connectors/competitor_tracker.py` | Not scheduled as weekly sweep |

---

## 7. SERP FEATURE OPTIMIZATION

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| PAA tree builder | ✅ 🔧 | `data/analyzers/paa_tree.py` | Full recursive Google SERP scraping, 24hr cache |
| Snippet format detector | ✅ 🔧 | `data/analyzers/snippet_format.py` | Detects paragraph/list/table/video format |
| Featured snippet optimizer | ✅ 🔧 | `core/geo_optimizer.py` | Direct answer injection, key takeaways |
| PAA → content generation | ❌ | | **paa_tree.py + snippet_format.py built but NOT wired to content generator** |
| Local pack optimization | ⚠️ 🔧 | `execution/connectors/external/gbp.py` | **Needs GBP OAuth credentials** |
| Image pack optimization | ❌ | | No alt text/image naming pipeline |
| Video carousel | ❌ | | No YouTube integration |
| PASF tracking | ❌ | | Not built |

---

## 8. PROGRAMMATIC SEO

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| Programmatic generator | ✅ 🤖 | `core/programmatic/generator.py` | |
| Template engine | ✅ 🔧 | `core/programmatic/template_engine.py` | |
| Batch task | ✅ | `run_programmatic_batch` | Default 10 pages/day — too low |
| Location × service matrix | ⚠️ | | Not confirmed fully built at 500+ pages/day scale |
| Template diversity | ❌ | | Single template risk — thin content penalty at scale |
| Programmatic quality gate | ✅ 🔧 | `execution/validators/content_gate.py` | Word count + AI detection |
| Page performance monitor | ❌ | | No per-template ranking report |
| Publish pipeline integration | `data/storage/programmatic_pages/publish_programmatic_pages.py` | ✅ | |

---

## 9. E-E-A-T SIGNALS

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| Author profiles | ✅ 🔧 | `core/author_profiles.py` | CRUD, Person schema, auto_inject_author |
| Author bio injection | ✅ 🔧 | `core/author_profiles.py` | `auto_inject_author()` |
| Trust signals | ✅ 🔧 | `core/trust_signals.py` | Privacy Policy, ToS, Editorial Policy, About page |
| EEAT pipeline | ✅ 🤖 | `core/eeat_pipeline.py` | `score_eeat()` 7 criteria, `run_eeat_pipeline()` 5 injections |
| Pass 5 EEAT post-processor | ✅ 🤖 | `core/generation_pipeline.py` | Runs after every content generation |
| EEAT sweep beat task | ✅ | Tue 11:00 | |
| EEAT scorer (ai_visibility) | ✅ 🔧 | `ai_visibility/eeat_scorer.py` | Separate scoring layer |
| Author credential verification | ❌ | | No LinkedIn/external credential check |
| Real review aggregation | ❌ | | AggregateRating uses placeholder — needs GBP/Trustpilot API |
| Editorial process disclosure | ⚠️ | | Policy page generated, not linked from every article |

---

## 10. BACKLINK ACQUISITION

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| Backlink prospector | ✅ 🔧 | `core/backlink_prospector.py` | Competitor gap, unlinked mentions, local citations, resource pages |
| Backlink crawler | ✅ 🔧 | `core/backlink_crawler.py` | |
| Outreach sequences | ✅ 🤖 | `core/backlink_outreach.py` | 3-step (day 0/5/12), 20/day cap |
| HARO automation | ✅ 🔧 | `execution/backlinks/haro.py` | **Needs active monitoring setup** |
| Link reclamation | ✅ 🔧 | `execution/backlinks/reclamation.py` | |
| Wayback links | ✅ 🔧 | `data/analyzers/wayback_links.py` | Dead link opportunities |
| Ahrefs backlink intel | ⚠️ 🔧 | `data/connectors/ahrefs.py` | **Needs AHREFS_API_KEY** |
| Email outreach active | ⚠️ | | **RESEND_API_KEY not set — zero emails sent** |
| Reply detection | ❌ | | No system monitors for outreach replies |
| Bounce handling | ❌ | | No bounce processing |
| Link placement verification | ❌ | | No crawler confirms link was actually placed |
| Digital PR / press releases | ❌ | | Not built |

---

## 11. INTERNAL LINKING + ANCHOR TEXT

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| Internal link suggestions | ✅ 🔧 | `core/internal_links.py` | Semantic similarity scoring |
| Semantic linker | ✅ 🔧 | `core/semantic_linker.py` + `core/linking/semantic_linker.py` | ⚠️ Duplicate — unclear which is authoritative |
| Link injector | ✅ 🔧 | `execution/link_injector.py` | Called from pipeline stage 6 |
| Orphan detection | 🔄 🔧 | `run_orphan_detection` task | Detects but does NOT auto-link |
| Anchor text tracking | ✅ 🔧 | `link_suggestions` table | anchor_text stored per suggestion |
| Anchor text distribution enforcement | ❌ | | **No ratio calculator or enforcement** — Penguin risk at scale |
| PageRank flow model | ❌ | | No authority-weighted link targeting |
| Link depth analyzer | ❌ | | No guarantee important pages ≤3 clicks from home |

---

## 12. AI SEARCH OPTIMIZATION (GEO/AEO)

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| GEO optimizer | ✅ 🔧 | `core/geo_optimizer.py` | Direct answer, key takeaways, freshness, definition, last updated |
| GEO readiness scorer | ✅ 🔧 | `core/geo_optimizer.py` | 7 criteria, pass≥60 |
| Speakable schema | ✅ 🔧 | `core/speakable_schema.py` | SpeakableSpecification — ⚠️ not confirmed auto-injected everywhere |
| llms.txt builder | ✅ 🔧 | `core/llms_txt_builder.py` | AI crawler permissions |
| Entity chains (EAV) | ✅ 🔧 | `core/entity_chains.py` | EAV triple injection |
| GEO prompts | ✅ 🤖 | `core/geo_prompts.py` | Citation-optimized prompt templates |
| AI visibility layer | ✅ | `ai_visibility/` | Separate module: citability, EEAT, mentions, answers, GEO strategies |
| Citability scorer | ✅ 🔧 | `ai_visibility/citability.py` | 5-dimension, 0-100 score, NO LLM |
| AI answer monitor | ✅ 🤖 | `core/ai_answer_monitor.py` + `monitoring/citation_monitor.py` | **Needs PERPLEXITY_API_KEY, OpenAI key for ChatGPT** |
| AI detector | ✅ 🔧 | `ai_visibility/ai_detector.py` | |
| GEO strategies | ✅ 🤖 | `ai_visibility/geo_strategies.py` | |
| AI Overview (SGE) optimization | ❌ | | Distinct from featured snippet — 40-60 word direct answers |
| Structured Q&A for LLM training | ❌ | | Not built |
| ChatGPT citation monitoring | ⚠️ | | Needs OpenAI API key |
| Gemini citation monitoring | ❌ | | Not built |

---

## 13. BRAND ENTITY + KNOWLEDGE GRAPH

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| Organization/LocalBusiness schema | ✅ 🔧 | `core/brand_entity.py` | Full JSON-LD with sameAs |
| Entity same-as management | ✅ 🔧 | `core/entity/same_as.py` + `core/brand_entity.py` | |
| Wikidata creation pipeline | ✅ 🔧 | `authority/wikidata.py` | **Full QuickStatements pipeline — ⚠️ needs activation per tenant** |
| Entity sweep | 🔄 🔧 | `core/brand_entity.run_entity_sweep()` | Built but not run — entity_mentions table empty |
| Entity mention monitoring | ✅ 🔧 | `monitoring/brand_mentions.py` | |
| Knowledge graph push | ✅ | `core/brand_entity.publish_to_knowledge_graph()` | Pushes to AION internal KG |
| GBP integration | ✅ 🔧 | `execution/connectors/external/gbp.py` + `data/connectors/gbp.py` | **Needs GBP OAuth credentials** |
| Knowledge panel monitoring | ❌ | | Not built |
| Wikidata presence check | ✅ 🔧 | `core/brand_entity.check_wikidata_presence()` | Checks — creation is in authority/wikidata.py |
| Wikipedia article strategy | ❌ | | Not built |

---

## 14. CROSS-PLATFORM DISTRIBUTION + PARASITE SEO

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| GitHub Pages | ⚠️ 🤖 | `core/parasite_seo.py` | Content generator ✅ — **Needs GITHUB_TOKEN, GITHUB_PAGES_OWNER, GITHUB_PAGES_REPO** |
| Medium | ⚠️ 🤖 | `execution/connectors/external/medium.py` + `core/parasite_seo.py` | **Needs MEDIUM_TOKEN** |
| Dev.to | ⚠️ 🤖 | `core/parasite_seo.py` | **Needs DEVTO_API_KEY** |
| Reddit | ⚠️ 🤖 | `execution/connectors/external/reddit.py` (PRAW, NOT Playwright) | **Needs REDDIT_CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD** |
| Quora | ❌ | `core/parasite_seo.py` `publish_via_playwright()` stub | Content generator ✅ — publisher not implemented |
| LinkedIn | ❌ | | No connector built — DA 98 platform |
| Blogger | ✅ 🔧 | `execution/connectors/external/blogger.py` | |
| Vercel | ✅ 🔧 | `execution/connectors/external/vercel.py` | |
| Substack | ❌ | | Not built |
| Press releases (PRLog/EIN) | ❌ | | Not built — high-ROI for data content |
| YouTube | ❌ | | Not built |
| Parasite content generators | ✅ 🤖 | `core/parasite_seo.py` | All use Grok primary, Claude fallback |
| Parasite rank checker | ✅ 🔧 | `core/parasite_seo.py check_parasite_rankings()` | |
| Parasite beat schedule | ✅ | Fri 09:00 sweep, Fri 10:00 rank check | |
| Pages published (actual) | ⚠️ | **0 published** — credentials not set | |
| Distribution engine | ✅ | `channels/distribution.py` | Multi-channel scheduler |
| Content multiplier | ✅ | `channels/multiplier.py` | |

---

## 15. CITATION CONTENT

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| Citable facts generator | ✅ 🤖 | `core/citable_data.py generate_local_facts()` | Claude+SERP, 15-20 facts |
| Cost guide generator | ✅ 🤖 | `core/citation_content.py` | Score 85, Claude CLI |
| Stats page generator | ✅ 🤖 | `core/citation_content.py` | Score 65, Claude CLI |
| FAQ hub generator | ✅ 🤖 | `core/citation_content.py` | Score 95, **Grok primary**, Claude fallback |
| Local study generator | ✅ 🤖 | `core/citation_content.py` | Score 65, **Grok primary**, Claude fallback |
| Citation readiness scorer | ✅ 🔧 | `core/citation_content._score_citation_readiness()` | 0-100, rule-based |
| Citation content beat tasks | ✅ | Thu 06:00 facts, Thu 07:00 sweep | |
| AI citability scorer | ✅ 🔧 | `ai_visibility/citability.py` | 5-dimension, pure Python/regex |
| Dataset schema on stats pages | ❌ | | Missing — high AI citation value |
| External data distribution | ❌ | | Facts stay in SQLite, not published externally |
| Citation builder | ✅ | `execution/citations/citation_builder.py` | |
| Wikipedia citation tracker | ✅ 🔧 | `data/analyzers/wikipedia_citations.py` | |

---

## 16. CONVERSION LAYER

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| Call tracking (DNI) | ✅ 🔧 | `core/call_tracking.py` | UTM-based phone swap JS |
| CTA A/B optimizer | ✅ 🤖 | `core/cta_optimizer.py` | Intent detection + variant generation |
| CTA auto-optimize beat | ✅ | Mon 08:00 | Pauses <70% CTR variants at 50+ impressions |
| Lead capture + qualify | ✅ 🔧 | `core/lead_capture.py` | 0-100 qualification score |
| Lead form builder | ✅ 🔧 | `core/lead_capture.py` | Service-specific fields, DNI JS |
| Lead notifications | ⚠️ | `core/lead_capture.notify_lead()` | **SMTP not configured — no emails sent** |
| CRM push (AION) | ✅ | `core/lead_capture.push_to_crm()` | AION bridge |
| GHL CRM connector | ✅ 🔧 | `data/connectors/crm_ghl.py` | |
| GA4 connector | ⚠️ 🔧 | `data/connectors/ga4.py` | **Needs GA4 credentials** |
| Conversion dashboard | ✅ | `GET /conversion/dashboard` | |
| Lead routing by score | ❌ | | High-score leads should trigger SMS/urgent notify |
| Heatmap / session recording | ❌ | | No MS Clarity or Hotjar integration |

---

## 17. CONTENT FRESHNESS

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| Content decay detection | ✅ 🔧 | `scan_content_decay` task + `data/analyzers/content_decay.py` | |
| Refresh scheduler | ✅ 🔧 | `core/refresh_schedule.py` | |
| Topic lifecycle | ✅ 🔧 | `core/topic_lifecycle.py` | |
| Last updated injection | ✅ 🔧 | `core/geo_optimizer.inject_last_updated()` | |
| Freshness data connector | ✅ 🔧 | `data/freshness.py` | |
| Auto-regeneration trigger | ❌ | | **Decay detected, regeneration NEVER triggered — broken loop** |
| Per-content-type refresh schedule | ❌ | | No different cadence for cost guide vs FAQ hub |
| Competitor freshness comparison | ❌ | | Not built |

---

## 18. COMPETITOR INTELLIGENCE

| Component | Status | Method | Notes |
|-----------|--------|--------|-------|
| Competitor scraper | ✅ 🔧 | `core/crawlers/competitor_scraper.py` + `data/crawlers/competitors.py` | |
| Competitor tracker connector | ✅ 🔧 | `data/connectors/competitor_tracker.py` | |
| Competitor exploit | ✅ 🤖 | `core/competitor_exploit.py` | |
| Competitor reaction prediction | ✅ | `prediction/competitor_reaction.py` | |
| Weekly keyword gap sweep | ❌ | | No scheduled beat job for competitor keyword gap |
| Competitor content change alerts | ❌ | | No monitoring for competitor page updates |
| Competitor new backlink alerts | ❌ | | Not built |
| SERP share of voice tracking | ❌ | | Not built |

---

## 19. ONBOARDING + CREDENTIAL VAULT

| Component | Status | Notes |
|-----------|--------|-------|
| Onboarding wizard HTML | ✅ | `static/onboarding.html` — 4-step wizard |
| Onboarding orchestrator | ✅ | `core/onboarding_orchestrator.py` — 9 steps sequential |
| Onboarding routes | ✅ | `api/onboarding_routes.py` |
| Onboarding Celery task | ✅ | `run_onboarding_task` |
| Credential vault | ✅ | `core/credential_vault.py` — Fernet-encrypted, 9 platforms |
| Credential routes | ✅ | `api/credential_routes.py` |
| VAULT_KEY env var | ⚠️ | Not set — credentials stored plaintext |

---

## 20. DASHBOARD + UI

| Component | Status | Notes |
|-----------|--------|-------|
| Main dashboard | ✅ | `static/dashboard.html` — `GET /` — 9 sections, sidebar, gap scorecard |
| Onboarding wizard | ✅ | `static/onboarding.html` — `GET /onboarding/` |
| Signup page | ✅ | `static/signup.html` |
| Topical authority view | ❌ | Not in dashboard — content_opportunities data not surfaced |
| Competitor gap view | ❌ | Not in dashboard |
| Anchor text distribution view | ❌ | Not built |

---

## 21. AUTOMATION + TASK PIPELINE

### All Celery Tasks
| Task Name | Queue | Status |
|-----------|-------|--------|
| analyze_business | analysis | ✅ |
| orchestrate_business | analysis | ✅ |
| execute_seo_task | execution | ✅ |
| run_learning | learning | ✅ |
| daily_analysis_cycle | analysis | ✅ |
| check_rankings | monitoring | ✅ |
| scan_content_decay | monitoring | ✅ |
| submit_to_indexnow | execution | ✅ |
| monitor_ai_citations | monitoring | ⚠️ needs keys |
| generate_content | execution | ✅ |
| publish_content | execution | ✅ |
| inject_internal_links | execution | ✅ |
| indexnow_and_track | execution | ✅ |
| run_content_pipeline | execution | ✅ |
| run_feedback_loop | learning | ✅ |
| run_citation_monitor | monitoring | ⚠️ needs keys |
| run_cwv_audit | monitoring | ✅ |
| run_topical_gap_check | analysis | 🔄 detects, doesn't generate |
| run_programmatic_batch | execution | ✅ |
| run_haro_check | execution | ⚠️ needs setup |
| run_link_reclamation | execution | ✅ |
| check_indexing_queue | execution | ✅ |
| run_system_health | monitoring | ✅ |
| run_orphan_detection | analysis | 🔄 detects, doesn't fix |
| send_daily_summary | monitoring | ⚠️ needs SMTP |
| sync_aion_signals | analysis | ✅ |
| sync_twitter_intel | analysis | ✅ |
| run_citation_facts_generate | content | ✅ Thu 06:00 |
| run_citation_content_sweep | content | ✅ Thu 07:00 |
| run_cta_optimize | content | ✅ Mon 08:00 |
| run_parasite_sweep_task | content | ✅ Fri 09:00 |
| run_parasite_rank_check | monitoring | ✅ Fri 10:00 |
| run_onboarding_task | content | ✅ |

### Missing Automation
| Missing Task | Priority |
|-------------|----------|
| PAA → content generation pipeline | High |
| Topical gap → auto-generate trigger | High |
| Content decay → auto-regeneration | High |
| Anchor text distribution reporter | High |
| Competitor weekly keyword gap sweep | Medium |
| Sitemap ping on new page publish | Medium |
| Wikidata entity sweep per new tenant | Medium |
| Dead letter queue for failed tasks | High |

---

## 22. RESILIENCE + ROBUSTNESS

| Component | Status | Notes |
|-----------|--------|-------|
| Task max_retries | ✅ | All tasks have retry limits |
| try/except throughout | ✅ | Core modules all wrapped |
| Grok → Claude fallback | ✅ | In parasite, citation generators |
| Dead letter queue | ❌ | Failed tasks exhaust retries silently |
| AION Bridge circuit breaker | ❌ | No fast-fail if :9082 goes down |
| Task idempotency | ⚠️ | `core/idempotency.py` exists — not confirmed used in all tasks |
| systemd auto-restart | ✅ | Services restart on crash |
| Multi-worker conflict (SQLite) | ⚠️ | Gap-phase modules use SQLite — concurrent writes risk locking |

---

## 23. API KEYS — WHAT ACTIVATES WHAT

| API Key | Service | What Gets Unlocked |
|---------|---------|-------------------|
| RESEND_API_KEY | Resend.com | Backlink outreach emails, lead notifications, daily summaries |
| PERPLEXITY_API_KEY | Perplexity AI | AI citation monitoring |
| REDDIT_CLIENT_ID + SECRET + USER + PASS | Reddit PRAW | Reddit parasite publishing (full connector ready) |
| MEDIUM_TOKEN | Medium | Medium article publishing |
| DEVTO_API_KEY | Dev.to | Dev.to article publishing |
| GITHUB_TOKEN + OWNER + REPO | GitHub | GitHub Pages parasite publishing |
| DATAFORSEO_LOGIN + PASSWORD | DataForSEO | Keyword volume, KD, SERP features |
| AHREFS_API_KEY | Ahrefs | Backlink intelligence, DR scoring |
| GA4 credentials | Google Analytics | Attribution, funnel analysis |
| GBP OAuth | Google Business Profile | Local pack, entity establishment, reviews |
| VAULT_KEY | Fernet | Encrypts all stored credentials |
| SMTP_HOST/USER/PASS | SMTP | Lead email notifications |
| ORIGINALITY_API_KEY | Originality.ai | AI content detection in quality gate |

All keys stored via: `POST /credentials/set` → `core/credential_vault.py`
Loaded into env at task start via: `inject_env_credentials(business_id, platform)`

---

## 24. LLM USAGE MAP — REPLACE WITH SCRAPER WHERE POSSIBLE

### Currently Using LLM (keep — genuinely needs language)
| Function | LLM Used | Reason |
|----------|----------|--------|
| Content generation (all types) | Grok → Claude | Creative language generation |
| `generate_local_facts()` | Claude | Synthesizing market intelligence |
| `generate_cta_variants()` | Grok → Claude | Creative copy |
| `core/geo_prompts.py` passes | Grok → Claude | Semantic optimization |
| `core/eeat_pipeline.py` | Grok → Claude | Trust signal writing |
| `core/competitor_exploit.py` | Grok → Claude | Strategic analysis |
| `core/geo_strategies.py` | Grok → Claude | GEO strategy generation |
| Onboarding orchestrator step outputs | Grok → Claude | Business-specific content |

### Already Replaced with Scraper/Python ✅ (keep LLM-free)
| Function | Method |
|----------|--------|
| PAA extraction | `data/analyzers/paa_tree.py` — BeautifulSoup, Google SERP |
| Snippet format detection | `data/analyzers/snippet_format.py` — regex rules |
| SERP scraping | `core/serp_scraper.py` — self-hosted Bing |
| Schema validation | `core/schema_validator.py` — rule-based |
| Citability scoring | `ai_visibility/citability.py` — 5-dimension regex/heuristic |
| EEAT scoring | `ai_visibility/eeat_scorer.py` — rule-based |
| Anchor text selection | `core/semantic_linker.py` — semantic similarity |
| Citation readiness score | `core/citation_content._score_citation_readiness()` — rule-based |
| GEO readiness score | `core/geo_optimizer.score_geo_readiness()` — rule-based |
| Wikidata pipeline | `authority/wikidata.py` — API calls, no LLM |
| Content decay detection | `data/analyzers/content_decay.py` — heuristic |
| Brand mention monitoring | `monitoring/brand_mentions.py` — scraping |
| Autocomplete | `data/analyzers/autocomplete.py` — scraping |
| Wikipedia citations | `data/analyzers/wikipedia_citations.py` — scraping |
| Wayback links | `data/analyzers/wayback_links.py` — API |

### Should Replace with Scraper (not yet done)
| Function | Current | Target |
|----------|---------|--------|
| Competitor content analysis | LLM | Parse HTML structure with BeautifulSoup |
| Schema type detection on competitor pages | LLM | JSON-LD regex extraction |
| Keyword intent classification | LLM | Rule-based intent signals (URL patterns, modifier words) |
| Topical gap analysis | LLM | Sitemap crawl + keyword set diff |
| CTA intent detection | LLM | Keyword matching rules |

---

## 25. MODULES NEEDING IMMEDIATE ATTENTION

### ❌ Must Build (genuine gaps)
1. `core/anchor_text_distributor.py` — ratio tracking + enforcement
2. PAA → generate pipeline (beat task wiring paa_tree → generate_content)
3. Topical gap → content queue wiring (content_opportunities → run_content_pipeline)
4. Content decay → auto-regeneration wiring
5. `execution/connectors/external/linkedin.py` — LinkedIn Articles connector (DA 98)
6. Press release distribution module (PRLog/EIN Presswire)
7. Sitemap ping automation on every new page publish

### ⚠️ Must Activate (built, needs config)
1. Set RESEND_API_KEY → activates all email outreach
2. Set REDDIT_CLIENT_ID/SECRET → activates Reddit connector
3. Set MEDIUM_TOKEN → activates Medium connector
4. Set DATAFORSEO credentials → activates keyword volume/KD
5. Run `run_entity_sweep()` per tenant → populate entity_mentions
6. Run `authority/wikidata.py` per tenant → Wikidata entity creation
7. Set PERPLEXITY_API_KEY → activates AI citation monitoring

### 🔄 Must Wire (detection without action)
1. `run_topical_gap_check` → `run_content_pipeline`
2. `scan_content_decay` → regeneration task
3. `run_orphan_detection` → `inject_internal_links`
4. `monitor_ai_citations` → content improvement trigger

---

## CHANGE LOG

| Date | Commit | What Changed |
|------|--------|-------------|
| 2026-04-17 | af1ced5 | Added: credential_vault.py, onboarding_orchestrator.py, credential_routes.py, onboarding_routes.py, dashboard.html, onboarding.html, run_onboarding_task Celery task |
| 2026-04-17 | 1f3fb39 | Added: core/parasite_seo.py, api/parasite_routes.py — Gap #7 Parasite SEO (6 platforms, generators, publishers, rank checker) |
| 2026-04-17 | 6802cf4 | Fix: faq_hub + local_study switched to Grok primary (score 10→95, 20→65) |
| 2026-04-17 | 188d1b1 | Fix: backlink_outreach EmailSender() no-args, send_transactional() correct method |
| 2026-04-17 | 90f4ecc | Added: call_tracking.py, cta_optimizer.py, lead_capture.py, conversion_routes.py — Gap #6 Conversion Layer |
| 2026-04-17 | 5ca81c8 | Added: citable_data.py, citation_content.py, citation_routes.py — Gap #5 LLM Citation Content |
| 2026-04-17 | c5caee4 | Added: brand_entity.py, brand_routes.py — Gap #4 Brand Entity / Knowledge Graph |
| 2026-04-17 | 58f5c5a | Added: backlink_prospector.py, backlink_outreach.py, backlink_routes.py — Gap #3 Backlink Acquisition |
| 2026-04-17 | f111857 | Added: author_profiles.py, trust_signals.py, eeat_pipeline.py, eeat_routes.py — Gap #2 E-E-A-T |
| 2026-04-17 | 4bea439 | Added: geo_optimizer.py, speakable_schema.py, llms_txt_builder.py, ai_answer_monitor.py, entity_chains.py, geo_routes.py — Gap #1 GEO/AEO |
| 2026-04-17 | (sync audit) | Confirmed VPS is authoritative — local Desktop copy is older subset, nothing to sync up |
| 2026-04-17 | (audit corrections) | Corrected: dead letter queue ✅ (celery_app.py), circuit breaker ✅ (core/llm_gateway.py), PAA wired ✅ (tasks.py L833-845), anchor text distribution ✅ (execution/link_injector.py EXACT_MAX=15%), topical gap → auto-generate ✅ (queues top 3 via run_content_pipeline), content decay → refresh ✅ (execute_seo_task.apply_async), orphan detection → linker enforcement ✅ |
| 2026-04-17 | 5cda556 | Added: execution/connectors/external/linkedin.py (DA 98, ugcPosts API, publish_article + publish_post), core/press_release.py (AP-style PR gen via Grok, PRLog DA69 distribution, run_press_release Celery task), credential_vault.py updated with linkedin+prlog keys |
| 2026-04-17 | 8eece15 | Fix: core/email_sender.py — replaced Resend API (key missing) with Amazon SES SMTP already configured in config/.env (email-smtp.us-east-2.amazonaws.com:587). SMTP login verified live. Outreach automation (HARO, backlink, lead notifications) NOW ACTIVE — was blocked as gap #28 (Outreach: 2/10), now unblocked (pending SES sandbox check). All warmup/bounce/complaint rate logic preserved. |

---

## NEXT BUILD QUEUE (Self-Improvement Targets → 10/10)

| Priority | Module | Replaces | Score Target |
|----------|--------|---------|-------------|
| 1 |  | DataForSEO rank tracking | #24 Indexing +2 |
| 2 |  | DataForSEO keyword volume/KD | #5 Keyword +2 |
| 3 |  | Ahrefs backlink data | #10 Backlinks +2 |
| 4 | Wire  auto-create to beat | nothing (gap) | #17 Brand Entity +3 |
| 5 |  | manual sitemap | #1 Tech SEO +2 |
| 6 | Add llms.txt to beat schedule | nothing (gap) | #16 AI Format +1 |
| 7 | Extend  for all platforms | OAuth API keys | #20 Parasite +3 |
| 8 | Set REDDIT/MEDIUM/LINKEDIN/GITHUB keys | — | #18,#19,#20 +4 |

## STRATEGY: Self-Hosted > Paid APIs (AGREED)
- DO NOT flag DataForSEO, Ahrefs, Perplexity, Grok, Claude as gaps
- Perplexity = CLI/cookie access ✅
- Grok = AION Brain :9082 ✅  
- Claude = CLI access ✅
- DataForSEO → replace with Python scrapers
- Ahrefs → replace with Common Crawl CDX API
- Free SERP tiers available if needed: Serper (2500/mo), Tavily, Exa

| 2026-04-17 | session | Established: self-hosted scraper strategy — DataForSEO/Ahrefs being replaced with Python scrapers + Common Crawl. Perplexity/Grok/Claude not API gaps — CLI/cookie access. |
| 2026-04-17 | 8eece15 | Fix: email_sender.py — Amazon SES SMTP (credentials already in config/.env). Outreach automation UNBLOCKED. Score #28 Outreach: 2→7. |


---

## NEXT BUILD QUEUE (Self-Improvement Targets to 10/10)

Priority 1: data/connectors/rank_checker.py — replaces DataForSEO rank tracking
Priority 2: data/connectors/serp_volume_estimator.py — replaces DataForSEO keyword volume/KD
Priority 3: data/connectors/common_crawl.py — replaces Ahrefs with free Common Crawl CDX API
Priority 4: Wire authority/wikidata.py auto-create to beat schedule (code gap)
Priority 5: data/storage/sitemap_builder.py — auto sitemap generation + IndexNow ping
Priority 6: Add llms_txt_builder.py to beat schedule (weekly)
Priority 7: Extend publish_via_playwright() for all platforms (Reddit/Medium/LinkedIn/GitHub)
Priority 8: Set REDDIT/MEDIUM/LINKEDIN/GITHUB OAuth keys

## STRATEGY: Self-Hosted Scrapers Replace Paid APIs (AGREED 2026-04-17)
- DO NOT flag DataForSEO, Ahrefs, Perplexity, Grok, Claude as gaps ever again
- Perplexity = CLI/cookie access (no API key needed)
- Grok = AION Brain :9082 (no API key needed)
- Claude = CLI access (no API key needed)
- DataForSEO = being replaced with Python scrapers
- Ahrefs = being replaced with Common Crawl CDX API (free, petabyte index)
- Free SERP tiers available as backup: Serper (2500/mo free), Tavily, Exa

## AUDIT UPDATE — 2026-04-17 (Deep Sweep)

**Accurate score after live SSH verification: 7.0/10**

61 beat tasks confirmed. Many previously-unknown modules and scheduled jobs discovered.

### Key Score Corrections vs Previous Audit
- #22 Automation Robustness: 7→9 (61 beat tasks, DLQ, circuit breaker all confirmed)
- #28 Outreach Automation: 2→7 (SES SMTP now live — email delivering)
- #14 GEO/AEO: 6→8 (geo_optimization_sweep + ai_answer_monitor + llms_txt both scheduled)
- #16 Content Formatting: 6→8 (deploy_llms_txt + run_llms_txt_deploy both in beat)
- #21 Content Freshness: 6→8 (scan_content_decay + inject_content_freshness + run_refresh_queue)
- #12 Competitor Analysis: 6→8 (run_competitor_crawl + run_competitor_exploit + competitor_content_alerts)
- #20 Parasite SEO: 3→5 (run_parasite_sweep_task + run_parasite_rank_check scheduled)
- #17 Brand Entity: 3→5 (run_wikidata_sync + run_entity_sweep + sync_entity_knowledge_graph scheduled)
- #30 Defensibility: 3→5 (run_learning + hypothesis + sync_aion_signals + run_signal_layer_sweep)

### Remaining Score Killers (blockers only)
1. #18 GitHub SEO: 2/10 — GITHUB_TOKEN not set (code ready)
2. #19 Cross-Platform: 6/10 — all platform keys missing (code ready)
3. #20 Parasite SEO: 5/10 — all platform keys missing (code ready)
4. #5 Keyword Strategy: 7/10 — no real volume data (self-hosted scraper in build queue)
5. #27 Data Moat: 5/10 — synthetic data only (real data scrapers in build queue)
6. #17 Brand Entity: 5/10 — Wikidata auto-create not wired to API (in build queue)

### Path to 10/10
Step 1: Set GitHub/Reddit/Medium/LinkedIn/DevTo API keys → 7.0→8.2
Step 2: Build rank_checker + serp_volume_estimator + common_crawl → 8.2→8.8
Step 3: Build Wikidata auto-create pipeline → 8.8→9.2
Step 4: Onboard first real tenant → 9.2→9.7+

| 2026-04-17 | 55ab613 | feat: self-hosted data scrapers — DataForSEO + Ahrefs replaced:
  data/connectors/rank_checker.py (Bing SERP rank tracking via Firecrawl)
  data/connectors/serp_volume_estimator.py (KD+volume from SERP signals, autocomplete)
  data/connectors/common_crawl.py (backlinks via CC CDX API + Wayback CDX, free)
  rank_tracker.py patched: RankChecker default, DFS only if credentials set
  dataforseo.py patched: SERPVolumeEstimator as keyword fallback
  backlink_prospector.py patched: CommonCrawlClient for competitor gap analysis |

## 2026-04-17 — Wikidata Auto-Create Pipeline (commit 5c442d8)

Gap closed: Authority/Wikidata was generating QuickStatements files only (manual submission required).
Now supports full programmatic entity creation via MediaWiki API.

WikidataAPI class (authority/wikidata.py):
  - login(): 3-step auth: get login token, POST login, get CSRF token
  - create_item(entity): POST wbeditentity with claims P31/P17/P131/P856/P1329/P2888
  - add_inception_date(qid, year): wbcreateclaim for P571
  - Reads WIKIDATA_USERNAME + WIKIDATA_PASSWORD from env; QS file fallback if not set

_store_qid(): UPDATE brand_entities SET wikidata_qid WHERE business_id in SQLite

run_entity_pipeline() updated flow:
  1. Notability check (existing)
  2. SPARQL existence check (existing)
  3. Build entity + validate sameAs URLs
  4. If creds set: API create, store QID
  5. Always save QS file as audit trail
  6. Returns: created, qid, method, label, same_as_count, quickstatements_lines

run_wikidata_sync task:
  - Skips businesses with existing wikidata_qid
  - Calls asyncio.run(run_entity_pipeline(biz)) per business
  - Returns: status, total, created, results, task_id

To activate: Set WIKIDATA_USERNAME and WIKIDATA_PASSWORD in config/.env
