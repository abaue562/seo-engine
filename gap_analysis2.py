import sqlite3

c = sqlite3.connect('data/storage/seo_engine.db')
c.row_factory = sqlite3.Row

print('=== CITATION PAGES ===')
for r in c.execute('SELECT page_type, slug, citation_score FROM citation_pages ORDER BY citation_score DESC').fetchall():
    print(f'  [{r["citation_score"]}] {r["page_type"]:15s} {r["slug"]}')

print('=== KEYWORD RANKINGS ===')
for r in c.execute('SELECT keyword, position, engine FROM keyword_rankings ORDER BY position').fetchall():
    print(f'  #{r["position"]} {r["keyword"]} ({r["engine"]})')

print('=== BRAND ENTITIES ===')
for r in c.execute('SELECT brand_name, entity_strength, schema_published, kg_published FROM brand_entities').fetchall():
    print(f'  {r["brand_name"]}  strength={r["entity_strength"]}  schema={r["schema_published"]}  kg={r["kg_published"]}')

print('=== BACKLINK PROSPECTS ===')
for r in c.execute('SELECT opportunity_type, COUNT(*) as n, AVG(priority_score) as avg_score FROM backlink_prospects GROUP BY opportunity_type').fetchall():
    print(f'  {r["opportunity_type"]}: {r["n"]} prospects  avg_priority={round(r["avg_score"] or 0,1)}')

print('=== CITABLE FACTS BY CATEGORY ===')
for r in c.execute('SELECT category, COUNT(*) as n FROM citable_facts GROUP BY category').fetchall():
    print(f'  {r["category"]}: {r["n"]}')

print('=== SAME-AS LINKS ===')
for r in c.execute('SELECT same_as_url FROM entity_same_as LIMIT 10').fetchall():
    print(f'  {r["same_as_url"]}')

try:
    n = c.execute('SELECT COUNT(*) as n FROM ai_answer_log').fetchone()['n']
    print(f'=== AI ANSWER LOG: {n} entries ===')
except Exception as e:
    print(f'=== AI ANSWER LOG: {e} ===')

print('=== AUTHOR PROFILES ===')
for r in c.execute('SELECT name, credentials, expertise_areas FROM author_profiles').fetchall():
    print(f'  {r["name"]}  creds={r["credentials"][:40] if r["credentials"] else ""}')

print('=== TRACKING NUMBERS ===')
for r in c.execute('SELECT number, source, label FROM tracking_numbers').fetchall():
    print(f'  {r["number"]}  source={r["source"]}  label={r["label"]}')

print('=== LEADS ===')
for r in c.execute('SELECT name, service, qualified_score, source, status FROM leads ORDER BY qualified_score DESC').fetchall():
    print(f'  [{r["qualified_score"]}] {r["name"]}  service={r["service"]}  src={r["source"]}  status={r["status"]}')

print('=== SERP RESULTS CACHED ===')
for r in c.execute('SELECT keyword, engine, organic_count, created_at FROM serp_results ORDER BY created_at DESC LIMIT 5').fetchall():
    print(f'  {r["keyword"]}  ({r["engine"]})  organic={r["organic_count"]}')
