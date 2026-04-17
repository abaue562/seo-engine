import sys, json, time
sys.path.insert(0, "/opt/seo-engine")
import xmlrpc.client
from pathlib import Path

biz_file = Path("data/storage/businesses.json")
businesses = json.loads(biz_file.read_text())
queue_dir = Path("data/storage/programmatic_pages")

for biz in businesses:
    wp_url = biz.get("wp_site_url", "")
    if not wp_url:
        continue
    wp_url = wp_url.rstrip("/") + "/xmlrpc.php"
    user = biz.get("wp_username", "")
    pwd = biz.get("wp_app_password", "")
    if not user or not pwd:
        continue
    index_file = queue_dir / "blend_bright_lights_pages.json"
    if not index_file.exists():
        continue
    data = json.loads(index_file.read_text())
    client = xmlrpc.client.ServerProxy(wp_url)
    published = 0
    for page in data["pages"]:
        if page.get("status") == "published":
            continue
        try:
            pid = client.wp.newPost(1, user, pwd, {
                "post_title": page["title"],
                "post_content": page["content"],
                "post_status": "publish",
                "post_name": page["slug"],
                "post_type": "post",
            })
            page["status"] = "published"
            page["wp_post_id"] = str(pid)
            print("PUB [" + str(pid) + "] " + page["slug"])
            published += 1
            time.sleep(0.5)
        except Exception as e:
            print("ERR " + page["slug"] + ": " + str(e)[:60])
    index_file.write_text(json.dumps({"total": len(data["pages"]), "pages": data["pages"]}, indent=2))
    print("Published:", published)
