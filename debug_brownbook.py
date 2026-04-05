"""Debug Brownbook signup — screenshot at every step."""
import sys, asyncio, re, os
sys.stdout.reconfigure(encoding="utf-8")
os.chdir(os.path.dirname(os.path.abspath(__file__)))

async def main():
    print("=== BROWNBOOK DEBUG ===")
    from execution.connectors.external.auto_signup import AutoSignupEngine
    engine = AutoSignupEngine()
    email, pw = await engine.create_email()
    print(f"Email: {email}")

    try:
        from patchright.async_api import async_playwright
    except ImportError:
        from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        await page.goto("https://www.brownbook.net/add-business/", timeout=20000)
        await page.wait_for_timeout(3000)
        await page.screenshot(path="debug_screenshots/bb_1_loaded.png")
        print(f"1. Loaded: {page.url}")

        # All visible fields
        fields = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('input, select, textarea')).filter(
                el => el.offsetParent !== null
            ).map(el => ({
                tag: el.tagName.toLowerCase(),
                type: (el.type || '').toLowerCase(),
                name: (el.name || '').toLowerCase(),
                id: (el.id || '').toLowerCase(),
                placeholder: (el.placeholder || '').toLowerCase(),
            }))
        }""")
        print(f"2. Fields ({len(fields)}):")
        for f in fields:
            print(f"   {f['tag']} type={f['type']} name={f['name']} ph={f['placeholder'][:40]}")

        # All dropdowns with options
        selects = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('select')).filter(
                el => el.offsetParent !== null
            ).map(el => ({
                name: el.name, id: el.id,
                options: Array.from(el.options).slice(0, 8).map(o => o.text.trim())
            }))
        }""")
        print(f"3. Dropdowns ({len(selects)}):")
        for s in selects:
            print(f"   name={s['name']} options={s['options']}")

        # All checkboxes
        boxes = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('input[type=checkbox]')).map(el => ({
                name: el.name, checked: el.checked,
                visible: el.offsetParent !== null,
                label: el.parentElement ? el.parentElement.innerText.trim().substring(0, 80) : ''
            }))
        }""")
        print(f"4. Checkboxes ({len(boxes)}):")
        for b in boxes:
            print(f"   name={b['name']} checked={b['checked']} vis={b['visible']} label='{b['label']}'")

        # Fill key fields manually
        fill_map = [
            ('input[placeholder*="business name" i]', "Blend Bright Lights"),
            ('input[placeholder*="business address" i], input[name="address"]', "123 Main St"),
            ('input[placeholder*="city" i], input[name="city"]', "Kelowna"),
            ('input[placeholder*="zip" i], input[name*="zip"]', "V1Y 1A1"),
            ('input[placeholder*="phone number" i], input[name="phone"]', "(250) 555-0199"),
            ('input[type="email"], input[name="email"]', email),
            ('input[placeholder*="website" i], input[name="website"]', "https://blendbrightlights.com"),
        ]
        for sel, val in fill_map:
            try:
                el = page.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    await el.fill(val)
                    print(f"   Filled: {sel[:40]} = {val[:30]}")
            except Exception:
                pass

        # Handle dropdowns (select country, etc)
        for s in selects:
            try:
                dropdown = page.locator(f"select[name='{s['name']}']").first
                if await dropdown.count() > 0 and await dropdown.is_visible():
                    opts = [o.lower() for o in s["options"]]
                    if any("canada" in o for o in opts):
                        await dropdown.select_option(label="Canada")
                        print(f"   Selected: {s['name']} = Canada")
                    elif any("british" in o for o in opts):
                        await dropdown.select_option(label="British Columbia")
                        print(f"   Selected: {s['name']} = British Columbia")
            except Exception as e:
                print(f"   Dropdown err: {s['name']} {e}")

        await page.screenshot(path="debug_screenshots/bb_2_filled.png")

        # Check all checkboxes
        try:
            all_boxes = page.locator('input[type="checkbox"]')
            for i in range(await all_boxes.count()):
                box = all_boxes.nth(i)
                if await box.is_visible() and not await box.is_checked():
                    await box.check()
                    print(f"   Checked checkbox {i}")
        except Exception:
            pass

        # Click submit
        clicked = False
        for btn_text in ["Next", "Submit", "Add", "Create", "Continue", "Save"]:
            try:
                btn = page.get_by_role("button", name=re.compile(btn_text, re.IGNORECASE)).first
                if await btn.count() > 0 and await btn.is_visible():
                    await btn.click()
                    clicked = True
                    await page.wait_for_timeout(5000)
                    print(f"5. Clicked: {btn_text}")
                    break
            except Exception:
                pass
        if not clicked:
            try:
                sub = page.locator('button[type="submit"], input[type="submit"]').first
                if await sub.count() > 0:
                    await sub.click()
                    clicked = True
                    await page.wait_for_timeout(5000)
                    print("5. Clicked generic submit")
            except Exception:
                pass

        await page.screenshot(path="debug_screenshots/bb_3_after_submit.png")
        print(f"6. URL: {page.url}")
        print(f"6. Title: {await page.title()}")

        # Check what's on the page now
        body = (await page.inner_text("body"))[:2000]
        for line in body.split("\n"):
            line = line.strip()
            if line and len(line) > 5 and any(k in line.lower() for k in ["error", "required", "please", "invalid", "missing", "success", "thank", "confirm", "verify", "step", "next"]):
                print(f"   PAGE: {line[:100]}")

        # Check if there's a new form (step 2?)
        new_fields = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('input, select, textarea')).filter(
                el => el.offsetParent !== null
            ).map(el => ({
                tag: el.tagName.toLowerCase(),
                type: (el.type || '').toLowerCase(),
                name: (el.name || '').toLowerCase(),
                placeholder: (el.placeholder || '').toLowerCase(),
            }))
        }""")
        if new_fields:
            print(f"7. New fields after submit ({len(new_fields)}):")
            for f in new_fields[:10]:
                print(f"   {f['tag']} type={f['type']} name={f['name']} ph={f['placeholder'][:40]}")

        await browser.close()

asyncio.run(main())
