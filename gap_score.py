import sqlite3, json

c = sqlite3.connect('data/storage/seo_engine.db')
c.row_factory = sqlite3.Row

print('\n' + '='*60)
print('  SEO ENGINE — GAP ANALYSIS SCORECARD')
print('='*60)

# ── GAP 1: E-E-A-T ──────────────────────────────────────────
authors = c.execute('SELECT COUNT(*) as n FROM author_profiles').fetchone()['n']
assignments = c.execute('SELECT COUNT(*) as n FROM author_assignments').fetchone()['n']
author_rows = c.execute('SELECT name, credentials FROM author_profiles').fetchall()
score1 = min(100, authors * 30 + (20 if assignments > 0 else 0) + 30)
print(f'\n[GAP 1] E-E-A-T Layer                   Score: {score1}/100')
print(f'  Authors: {authors}  |  Assignments: {assignments}')
for r in author_rows:
    print(f'  Author: {r["name"]}  creds={str(r["credentials"])[:50]}')
print(f'  Status: {"✓ LIVE" if score1>=60 else "⚠ NEEDS DATA"}')

# ── GAP 2: BACKLINKS ────────────────────────────────────────
prospects = c.execute('SELECT COUNT(*) as n FROM backlink_prospects').fetchone()['n']
acquired = c.execute('SELECT COUNT(*) as n FROM backlink_acquired').fetchone()['n']
outreach = c.execute('SELECT COUNT(*) as n FROM outreach_log').fetchone()['n']
score2 = min(100, prospects * 5 + acquired * 20 + (20 if outreach > 0 else 0))
print(f'\n[GAP 2] Backlink Acquisition              Score: {score2}/100')
print(f'  Prospects: {prospects}  |  Acquired: {acquired}  |  Outreach sent: {outreach}')
for r in c.execute('SELECT opportunity_type, COUNT(*) as n FROM backlink_prospects GROUP BY opportunity_type').fetchall():
    print(f'  Type: {r["opportunity_type"]}: {r["n"]}')
print(f'  Status: {"✓ LIVE" if score2>=40 else "⚠ NEEDS OUTREACH"}')

# ── GAP 3: BRAND ENTITY ─────────────────────────────────────
entities = c.execute('SELECT COUNT(*) as n FROM brand_entities').fetchone()['n']
same_as = c.execute('SELECT COUNT(*) as n FROM entity_same_as').fetchone()['n']
mentions = c.execute('SELECT COUNT(*) as n FROM entity_mentions').fetchone()['n']
entity_row = c.execute('SELECT entity_name, entity_score FROM brand_entities LIMIT 1').fetchone()
score3 = min(100, entities * 20 + same_as * 15 + mentions * 5 + (entity_row["entity_score"] or 0) if entity_row else 0)
print(f'\n[GAP 3] Brand Entity / Knowledge Graph    Score: {score3}/100')
print(f'  Entities: {entities}  |  Same-As: {same_as}  |  Mentions: {mentions}')
if entity_row:
    print(f'  Entity: {entity_row["entity_name"]}  entity_score={entity_row["entity_score"]}')
    for r in c.execute('SELECT same_as_url FROM entity_same_as LIMIT 5').fetchall():
        print(f'  SameAs: {r["same_as_url"]}')
print(f'  Status: {"✓ LIVE" if score3>=40 else "⚠ NEEDS MORE SAME-AS"}')

# ── GAP 4: SERP/CRAWL ────────────────────────────────────────
serp_count = c.execute('SELECT COUNT(*) as n FROM serp_results').fetchone()['n']
rank_count = c.execute('SELECT COUNT(*) as n FROM keyword_rankings').fetchone()['n']
kw_count = c.execute('SELECT COUNT(*) as n FROM keyword_intel').fetchone()['n']
rank_row = c.execute('SELECT keyword, position, engine FROM keyword_rankings LIMIT 1').fetchone()
score4 = min(100, serp_count * 5 + rank_count * 10 + kw_count * 10 + 30)
print(f'\n[GAP 4] Self-hosted SERP/Crawl Layer      Score: {score4}/100')
print(f'  SERP cached: {serp_count}  |  Rankings tracked: {rank_count}  |  KW Intel: {kw_count}')
if rank_row:
    pos = rank_row["position"] or "not ranked"
    print(f'  Top tracking: "{rank_row["keyword"]}" — pos {pos} on {rank_row["engine"]}')
print(f'  Status: {"✓ LIVE" if score4>=50 else "⚠ RUN RANK SWEEP"}')

# ── GAP 5: LLM CITATION CONTENT ─────────────────────────────
facts = c.execute('SELECT COUNT(*) as n FROM citable_facts').fetchone()['n']
pages = c.execute('SELECT COUNT(*) as n FROM citation_pages').fetchone()['n']
avg_score_row = c.execute('SELECT AVG(citation_score) as s FROM citation_pages').fetchone()
avg_score = round(avg_score_row['s'] or 0, 1)
score5 = min(100, facts * 2 + pages * 5 + int(avg_score))
print(f'\n[GAP 5] LLM Citation Content Layer        Score: {score5}/100')
print(f'  Citable facts: {facts}  |  Pages: {pages}  |  Avg citation score: {avg_score}')
for r in c.execute('SELECT page_type, citation_score, slug FROM citation_pages ORDER BY citation_score DESC').fetchall():
    print(f'  [{r["citation_score"]:3d}] {r["page_type"]:15s}  {r["slug"]}')
print(f'  Status: {"✓ LIVE" if score5>=60 else "⚠ GENERATE MORE PAGES"}')

# ── GAP 6: CONVERSION ────────────────────────────────────────
numbers = c.execute('SELECT COUNT(*) as n FROM tracking_numbers').fetchone()['n']
calls = c.execute('SELECT COUNT(*) as n FROM call_log').fetchone()['n']
leads = c.execute('SELECT COUNT(*) as n FROM leads').fetchone()['n']
hot = c.execute('SELECT COUNT(*) as n FROM leads WHERE qualified_score>=70').fetchone()['n']
ctas = c.execute('SELECT COUNT(*) as n FROM cta_variants').fetchone()['n']
lead_row = c.execute('SELECT name, qualified_score, source, service FROM leads ORDER BY qualified_score DESC LIMIT 1').fetchone()
score6 = min(100, numbers * 15 + leads * 20 + hot * 10 + ctas * 10 + (calls > 0) * 10)
print(f'\n[GAP 6] Conversion Layer                  Score: {score6}/100')
print(f'  Tracking numbers: {numbers}  |  Calls: {calls}  |  Leads: {leads}  |  Hot: {hot}  |  CTAs: {ctas}')
if lead_row:
    print(f'  Top lead: {lead_row["name"]}  score={lead_row["qualified_score"]}  src={lead_row["source"]}  svc={lead_row["service"]}')
print(f'  Status: {"✓ LIVE" if score6>=50 else "⚠ GENERATE CTA VARIANTS"}')

# ── OVERALL ──────────────────────────────────────────────────
overall = round((score1 + score2 + score3 + score4 + score5 + score6) / 6)
print('\n' + '='*60)
print(f'  OVERALL SEO ENGINE SCORE: {overall}/100')
print('='*60)

gaps = [
    ('E-E-A-T', score1, 'Low — run eeat sweep per tenant'),
    ('Backlinks', score2, 'Low — start outreach queue'),
    ('Brand Entity', score3, 'Med — add more sameAs + mentions'),
    ('SERP/Crawl', score4, 'Med — run rank tracking sweep'),
    ('Citation Content', score5, 'High — faq_hub score 10 needs fixing'),
    ('Conversion', score6, 'Med — generate CTA variants'),
]
print('\n  PRIORITIES:')
for name, s, note in sorted(gaps, key=lambda x: x[1]):
    bar = '█' * (s // 10) + '░' * (10 - s // 10)
    print(f'  {bar} {s:3d}  {name:20s}  {note}')
print()
