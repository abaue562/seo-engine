import sqlite3, json

c = sqlite3.connect('data/storage/seo_engine.db')
c.row_factory = sqlite3.Row

rows = c.execute('SELECT page_type, slug, citation_score FROM citation_pages ORDER BY citation_score DESC').fetchall()
print('=== CITATION PAGES ===')
for r in rows:
    print(f'  [{r["citation_score"]}] {r["page_type"]:15s} {r["slug"]}')

rows = c.execute('SELECT keyword, position, domain FROM keyword_rankings ORDER BY position').fetchall()
print('=== KEYWORD RANKINGS ===')
for r in rows:
    print(f'  #{r["position"]} {r["keyword"]} ({r["domain"]})')

rows = c.execute('SELECT brand_name, entity_strength, schema_published, kg_published FROM brand_entities').fetchall()
print('=== BRAND ENTITIES ===')
for r in rows:
    print(f'  {r["brand_name"]} strength={r["entity_strength"]} schema={r["schema_published"]} kg={r["kg_published"]}')

rows = c.execute('SELECT opportunity_type, COUNT(*) as n, AVG(priority_score) as avg_score FROM backlink_prospects GROUP BY opportunity_type').fetchall()
print('=== BACKLINK PROSPECTS ===')
for r in rows:
    print(f'  {r["opportunity_type"]}: {r["n"]} prospects avg_priority={round(r["avg_score"] or 0,1)}')

rows = c.execute('SELECT category, COUNT(*) as n FROM citable_facts GROUP BY category').fetchall()
print('=== CITABLE FACTS BY CATEGORY ===')
for r in rows:
    print(f'  {r["category"]}: {r["n"]}')

rows = c.execute('SELECT same_as_url FROM entity_same_as LIMIT 10').fetchall()
print('=== SAME-AS LINKS ===')
for r in rows:
    print(f'  {r["same_as_url"]}')

# GEO / AEO layer check
try:
    rows = c.execute('SELECT COUNT(*) as n FROM ai_answer_log').fetchone()
    print(f'=== AI ANSWER LOG: {rows["n"]} entries ===')
except:
    print('=== AI ANSWER LOG: table missing ===')

# Check geo_optimizer results table
try:
    rows = c.execute('SELECT COUNT(*) as n FROM geo_scores').fetchone()
    print(f'=== GEO SCORES: {rows["n"]} ===')
except:
    print('=== GEO SCORES: table not found (inline) ===')

# eeat scores
try:
    rows = c.execute('SELECT COUNT(*) as n, AVG(score) as avg FROM eeat_scores').fetchone()
    print(f'=== EEAT SCORES: {rows["n"]} entries, avg={round(rows["avg"] or 0,1)} ===')
except:
    print('=== EEAT SCORES: table not found ===')
