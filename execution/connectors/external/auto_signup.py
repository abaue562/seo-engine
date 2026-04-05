"""Auto-Signup Engine — registers on third-party websites with zero human intervention.

Pipeline:
  1. Faker generates identity (or uses real business info)
  2. pymailtm creates disposable email with real inbox
  3. browser-use (AI) or Playwright fills signup form
  4. CAPTCHA auto-solved (ddddocr for text, pypasser for reCAPTCHA)
  5. pymailtm polls for confirmation email
  6. Extract verification link/code
  7. Complete verification

CAPTCHA solving stack:
  - ddddocr: text/image CAPTCHAs (local ML, 13K stars)
  - pypasser: reCAPTCHA v2 audio bypass + v3 token generation
  - Playwright-based: Cloudflare Turnstile (undetected browser)
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import base64
from datetime import datetime
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)


# =====================================================================
# CAPTCHA Solvers
# =====================================================================

class CaptchaSolver:
    """Multi-method CAPTCHA solver — tries free local methods first, then fallbacks."""

    @staticmethod
    async def solve_text_captcha(image_bytes: bytes) -> str:
        """Solve text/image CAPTCHA using ddddocr (local ML)."""
        try:
            import ddddocr
            ocr = ddddocr.DdddOcr(show_ad=False)
            result = ocr.classification(image_bytes)
            log.info("captcha.text_solved  result=%s", result)
            return result
        except Exception as e:
            log.error("captcha.text_fail  err=%s", e)
            return ""

    @staticmethod
    async def solve_recaptcha_v2(site_url: str, page=None) -> str:
        """Solve reCAPTCHA v2 — tries playwright-recaptcha (audio), then pypasser fallback."""
        # Method 1: playwright-recaptcha (best — native Playwright, free, unlimited)
        if page:
            try:
                from playwright_recaptcha import recaptchav2
                async with recaptchav2.AsyncSolver(page) as solver:
                    token = await solver.solve_recaptcha()
                    if token:
                        log.info("captcha.recaptcha_v2_solved  method=playwright-recaptcha  token=%s", token[:30])
                        return token
            except Exception as e:
                log.warning("captcha.playwright_recaptcha_fail  err=%s", e)

        # Method 2: pypasser audio bypass (fallback)
        try:
            from pypasser import reCaptchaV2
            token = reCaptchaV2(site_url)
            log.info("captcha.recaptcha_v2_solved  method=pypasser  token=%s", token[:30] if token else "None")
            return token or ""
        except Exception as e:
            log.error("captcha.recaptcha_v2_fail  err=%s", e)
            return ""

    @staticmethod
    async def solve_recaptcha_v3(site_url: str, action: str = "submit") -> str:
        """Solve reCAPTCHA v3 using pypasser token generation."""
        try:
            from pypasser import reCaptchaV3
            token = reCaptchaV3(site_url, action)
            log.info("captcha.recaptcha_v3_solved")
            return token or ""
        except Exception as e:
            log.error("captcha.recaptcha_v3_fail  err=%s", e)
            return ""

    @staticmethod
    async def solve_slider_captcha(background_bytes: bytes, slider_bytes: bytes) -> int:
        """Solve slider CAPTCHA using ddddocr (detect gap position)."""
        try:
            import ddddocr
            det = ddddocr.DdddOcr(det=False, ocr=False, show_ad=False)
            result = det.slide_match(slider_bytes, background_bytes)
            x_position = result.get("target", [0, 0])[0]
            log.info("captcha.slider_solved  x=%d", x_position)
            return x_position
        except Exception as e:
            log.error("captcha.slider_fail  err=%s", e)
            return 0

    async def detect_and_solve(self, page) -> dict:
        """Detect CAPTCHA type on a Playwright page and solve it."""
        result = {"type": "none", "solved": False}

        try:
            html = await page.content()

            # Check for reCAPTCHA
            if "recaptcha" in html.lower() or "g-recaptcha" in html.lower():
                result["type"] = "recaptcha_v2"

                # Method 1: Direct iframe click (simplest — works with Patchright stealth)
                try:
                    recaptcha_frame = page.frame_locator('iframe[src*="recaptcha"]')
                    checkbox = recaptcha_frame.locator('#recaptcha-anchor')
                    if await checkbox.count() > 0:
                        await checkbox.click()
                        # Wait up to 15 seconds for it to solve
                        for _ in range(15):
                            await page.wait_for_timeout(1000)
                            try:
                                is_checked = await recaptcha_frame.locator('.recaptcha-checkbox-checked').count()
                                if is_checked > 0:
                                    result["solved"] = True
                                    log.info("captcha.recaptcha_v2_solved  method=direct_click")
                                    return result
                            except Exception:
                                pass
                        log.info("captcha.recaptcha_v2_clicked_but_not_solved  waiting_for_challenge")
                except Exception as e:
                    log.debug("captcha.direct_click_fail  err=%s", e)

                # Method 2: playwright-recaptcha library
                try:
                    from playwright_recaptcha import recaptchav2
                    async with recaptchav2.AsyncSolver(page) as solver:
                        token = await solver.solve_recaptcha(timeout=30)
                        if token:
                            result["solved"] = True
                            result["token"] = token[:30]
                            log.info("captcha.recaptcha_v2_solved  method=playwright-recaptcha")
                            return result
                except Exception as e:
                    log.debug("captcha.playwright_recaptcha_fail  err=%s", e)

                # Method 3: pypasser fallback
                try:
                    url = page.url
                    token = await self.solve_recaptcha_v2(url, page=None)
                    if token:
                        try:
                            await page.evaluate(f'document.getElementById("g-recaptcha-response").innerHTML = "{token}"')
                        except Exception:
                            pass
                        result["solved"] = True
                        result["token"] = token[:30]
                except Exception:
                    pass

                return result

            # Check for image CAPTCHA
            captcha_img = page.locator('img[src*="captcha"], img[alt*="captcha"], img[class*="captcha"]')
            if await captcha_img.count() > 0:
                result["type"] = "text_image"
                # Screenshot the captcha image
                img_element = captcha_img.first
                img_bytes = await img_element.screenshot()
                text = await self.solve_text_captcha(img_bytes)
                if text:
                    # Find the input field near the captcha and fill it
                    captcha_input = page.locator('input[name*="captcha"], input[placeholder*="captcha"], input[id*="captcha"]')
                    if await captcha_input.count() > 0:
                        await captcha_input.first.fill(text)
                        result["solved"] = True
                        result["answer"] = text
                return result

            # Check for Cloudflare Turnstile
            if "turnstile" in html.lower() or "cf-turnstile" in html.lower():
                result["type"] = "turnstile"
                # Turnstile often solves itself in undetected browsers — just wait
                await page.wait_for_timeout(5000)
                result["solved"] = True
                result["note"] = "waited for auto-solve"
                return result

        except Exception as e:
            log.error("captcha.detect_fail  err=%s", e)
            result["error"] = str(e)

        return result


class SignupIdentity(BaseModel):
    """Identity used for signup — real business info + disposable email."""
    business_name: str
    contact_name: str = ""
    email: str = ""                    # Disposable email from pymailtm
    phone: str = ""
    website: str = ""
    address: str = ""
    city: str = ""
    description: str = ""
    category: str = ""
    password: str = ""


class SignupResult(BaseModel):
    site: str
    url: str = ""
    status: str = "pending"            # pending / email_created / form_filled / verified / failed
    email_used: str = ""
    confirmation_received: bool = False
    verification_completed: bool = False
    error: str = ""
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class AutoSignupEngine:
    """Fully automated website registration."""

    def __init__(self):
        self._email_account = None       # pymailtm account
        self._mail_tm_token = ""          # mail.tm API JWT
        self._mail_tm_email = ""          # mail.tm email address
        self._mail_tm_password = ""       # mail.tm password for re-auth
        self._mail_tm_session = None      # requests.Session for consistent connection
        self._guerrilla_sid = ""          # guerrilla mail session

    # ---- Step 1: Create disposable email ----

    async def create_email(self) -> tuple[str, str]:
        """Create a disposable email address. Tries pymailtm, falls back to mail.tm API directly."""
        # Method 1: Direct mail.tm API with requests.Session (MOST RELIABLE)
        try:
            import requests as req
            from faker import Faker
            fake = Faker()

            s = req.Session()
            self._mail_tm_session = s

            # Get domains
            domains_resp = s.get("https://api.mail.tm/domains", timeout=10)
            domains = domains_resp.json().get("hydra:member", [])
            if not domains:
                raise ValueError("No domains")
            domain = domains[0]["domain"]

            # Create account
            username = fake.user_name() + str(int(time.time()) % 99999)
            email = f"{username}@{domain}"
            password = fake.password(length=12, special_chars=False)

            create_resp = s.post("https://api.mail.tm/accounts", json={"address": email, "password": password}, timeout=10)
            if create_resp.status_code not in (200, 201):
                raise ValueError(f"Create failed: {create_resp.status_code}")

            # Get token with SAME session
            token_resp = s.post("https://api.mail.tm/token", json={"address": email, "password": password}, timeout=10)
            if token_resp.status_code != 200:
                raise ValueError(f"Token failed: {token_resp.status_code}")

            self._mail_tm_token = token_resp.json().get("token", "")
            self._mail_tm_email = email
            self._mail_tm_password = password

            # Verify inbox works
            inbox_resp = s.get("https://api.mail.tm/messages", headers={"Authorization": f"Bearer {self._mail_tm_token}"}, timeout=10)
            inbox_ok = inbox_resp.status_code == 200

            log.info("signup.email_created  method=mail.tm_session  email=%s  inbox_ok=%s", email, inbox_ok)
            return email, password
        except Exception as e:
            log.warning("signup.mailtm_session_fail  err=%s", e)

        # Method 3: Guerrilla Mail API (always works)
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get("https://api.guerrillamail.com/ajax.php?f=get_email_address")
                data = resp.json()
                email = data.get("email_addr", "")
                self._guerrilla_sid = data.get("sid_token", "")
                if email:
                    log.info("signup.email_created  method=guerrilla  email=%s", email)
                    return email, "guerrilla"
        except Exception as e:
            log.warning("signup.guerrilla_fail  err=%s", e)

        # Final fallback: generate a realistic but non-functional email
        from faker import Faker
        fake = Faker()
        email = fake.email()
        log.error("signup.email_all_methods_failed  using_fake=%s", email)
        return email, "placeholder"

    # ---- Step 2: Generate identity ----

    def create_identity(
        self,
        business_name: str,
        website: str = "",
        city: str = "",
        service: str = "",
        phone: str = "",
        description: str = "",
        email: str = "",
    ) -> SignupIdentity:
        """Create signup identity using real business info + disposable email."""
        from faker import Faker
        fake = Faker("en_CA")  # Canadian locale

        password = fake.password(length=14, special_chars=True, digits=True, upper_case=True)
        contact = fake.name()

        identity = SignupIdentity(
            business_name=business_name,
            contact_name=contact,
            email=email,
            phone=phone or fake.phone_number(),
            website=website,
            address=fake.street_address() if not city else f"123 Main St",
            city=city,
            description=description[:500] if description else f"{business_name} provides professional {service} services in {city}.",
            category=service,
            password=password,
        )

        log.info("signup.identity  name=%s  email=%s", business_name, email)
        return identity

    # ---- Step 3: Fill signup form (browser-use AI) ----

    async def fill_form_ai(self, url: str, identity: SignupIdentity) -> dict:
        """Use browser-use AI to fill a signup form on any website."""
        try:
            from browser_use import Agent
        except ImportError:
            log.error("signup.browseruse_missing")
            return {"status": "error", "reason": "browser-use not installed or missing LLM provider"}

        task = f"""Go to {url}

Look for a "Sign Up", "Register", "Add Business", "Claim Listing", or "Create Account" button or link. Click it.

Fill in the registration form with these details:
- Business Name: {identity.business_name}
- Name / Contact: {identity.contact_name}
- Email: {identity.email}
- Password: {identity.password}
- Phone: {identity.phone}
- Website: {identity.website}
- City: {identity.city}
- Category: {identity.category}
- Description: {identity.description[:200]}

If any field is not available, skip it.
If there is a CAPTCHA, stop and report it.
Do NOT enter any payment or credit card information.
Submit the form when complete."""

        try:
            # browser-use needs an LLM — try to use what's available
            agent = Agent(task=task, llm=None)  # Will use default
            result = await agent.run()
            log.info("signup.form_filled  url=%s", url)
            return {"status": "filled", "result": str(result)[:500]}
        except Exception as e:
            log.error("signup.form_fail  url=%s  err=%s", url, e)
            return {"status": "failed", "error": str(e)}

    # ---- Step 3b: Fill form with Patchright (stealth) + multi-layer CAPTCHA solving ----

    async def fill_form_playwright(self, url: str, identity: SignupIdentity) -> dict:
        """Use Camoufox (best stealth) → Patchright → Playwright to fill signup form."""
        captcha_solver = CaptchaSolver()
        browser = None
        context = None
        use_camoufox = False

        # Try Camoufox first (deepest stealth — C++ level fingerprint spoofing)
        try:
            from camoufox.async_api import AsyncCamoufox
            log.info("signup.using_camoufox  stealth=max")
            camoufox_ctx = AsyncCamoufox(headless=False)
            browser = await camoufox_ctx.__aenter__()
            use_camoufox = True
            self._camoufox_ctx = camoufox_ctx
        except Exception as e:
            log.debug("signup.camoufox_fail  err=%s  falling back to patchright", e)
            try:
                from patchright.async_api import async_playwright
                log.info("signup.using_patchright  stealth=true")
            except ImportError:
                try:
                    from playwright.async_api import async_playwright
                    log.info("signup.using_playwright  stealth=false")
                except ImportError:
                    return {"status": "error", "reason": "no browser engine available"}

            pw = await async_playwright().__aenter__()
            self._pw_ctx = pw
            browser = await pw.chromium.launch(headless=False)

        try:
            if use_camoufox:
                page = await browser.new_page()
            else:
                context = await browser.new_context()
                page = await context.new_page()

            await page.goto(url, timeout=20000)
            await page.wait_for_timeout(3000)

            # ---- UNIVERSAL POPUP/BANNER DISMISSER ----
            # Handle cookie banners, notifications, popups BEFORE doing anything
            dismiss_texts = [
                "Accept All", "Accept all cookies", "Accept Cookies", "Accept",
                "Allow All", "Allow all", "Allow", "Got it", "I Accept",
                "OK", "Okay", "Dismiss", "Close", "No thanks", "Not now",
                "I understand", "Agree", "Continue", "Skip",
                "Maybe Later", "No Thanks", "Decline optional",
            ]
            for dt in dismiss_texts:
                try:
                    btn = page.get_by_role("button", name=re.compile(f"^{re.escape(dt)}$", re.IGNORECASE)).first
                    if await btn.count() > 0 and await btn.is_visible():
                        await btn.click()
                        await page.wait_for_timeout(500)
                        log.info("signup.dismissed  text=%s", dt)
                except Exception:
                    pass

            # Also close any modal overlays
            for close_sel in [
                'button[aria-label="Close"]', 'button[class*="close"]',
                '.modal-close', '[data-dismiss="modal"]', '.close-btn',
                'button:has-text("×")', 'button:has-text("✕")',
            ]:
                try:
                    close_btn = page.locator(close_sel).first
                    if await close_btn.count() > 0 and await close_btn.is_visible():
                        await close_btn.click()
                        await page.wait_for_timeout(500)
                        log.info("signup.closed_overlay  sel=%s", close_sel[:30])
                except Exception:
                    pass

            if True:  # unified flow for both browsers
                # Try to find and click signup/register button
                # Priority order: specific business signup first, then generic
                signup_texts = [
                    "Add your business", "Add Business", "List your business",
                    "Claim your listing", "Add a listing", "Add Listing",
                    "Register your business", "Create Listing",
                    "Sign Up", "Register", "Create Account",
                    "Get Started", "Join Free", "Join Now", "Join",
                    "Get Listed", "Free Listing",
                ]
                clicked_signup = False
                for btn_text in signup_texts:
                    try:
                        # Try links first (most signup buttons are <a> tags)
                        link = page.get_by_role("link", name=re.compile(btn_text, re.IGNORECASE)).first
                        if await link.count() > 0 and await link.is_visible():
                            await link.click()
                            await page.wait_for_timeout(3000)
                            log.info("signup.clicked_link  text=%s  new_url=%s", btn_text, page.url)
                            clicked_signup = True
                            break
                    except Exception:
                        pass
                    try:
                        # Then try buttons
                        btn = page.get_by_role("button", name=re.compile(btn_text, re.IGNORECASE)).first
                        if await btn.count() > 0 and await btn.is_visible():
                            await btn.click()
                            await page.wait_for_timeout(3000)
                            log.info("signup.clicked_button  text=%s", btn_text)
                            clicked_signup = True
                            break
                    except Exception:
                        pass
                    try:
                        # Fallback: any element with matching text
                        el = page.get_by_text(btn_text, exact=False).first
                        if await el.count() > 0 and await el.is_visible():
                            await el.click()
                            await page.wait_for_timeout(3000)
                            log.info("signup.clicked_text  text=%s", btn_text)
                            clicked_signup = True
                            break
                    except Exception:
                        pass

                # Wait for potential modal / overlay / page navigation
                if clicked_signup:
                    await page.wait_for_timeout(2000)
                    log.info("signup.after_click  url=%s", page.url)

                    # Detect multi-step: if page only has email+password, fill those first
                    visible_inputs = await page.evaluate('''() => {
                        return Array.from(document.querySelectorAll('input')).filter(
                            el => el.offsetParent !== null
                        ).map(el => ({type: el.type, name: el.name, id: el.id, placeholder: el.placeholder}))
                    }''')
                    input_types = [i.get("type", "") for i in visible_inputs]
                    log.info("signup.visible_inputs  count=%d  types=%s", len(visible_inputs), input_types)

                    # If only email+password visible → it's step 1 of multi-step signup
                    is_login_page = (
                        len(visible_inputs) <= 4
                        and any(i.get("type") == "email" or "email" in i.get("name", "").lower() for i in visible_inputs)
                        and any(i.get("type") == "password" for i in visible_inputs)
                    )

                    if is_login_page:
                        log.info("signup.multi_step_detected  filling email+password first")

                        # Look for "Sign up" / "Don't have an account" link/button
                        for signup_text in [
                            "Sign up here", "Sign up", "Don't have an account",
                            "Create account", "Register here", "New user",
                        ]:
                            try:
                                signup_link = page.get_by_text(signup_text, exact=False).first
                                if await signup_link.count() > 0 and await signup_link.is_visible():
                                    await signup_link.click()
                                    await page.wait_for_timeout(3000)
                                    log.info("signup.clicked_signup_link  text=%s  url=%s", signup_text, page.url)
                                    break
                            except Exception:
                                pass

                        # Now fill email + password
                        try:
                            email_field = page.locator('input[type="email"], input[name*="email" i], input[id*="email" i]').first
                            if await email_field.count() > 0 and await email_field.is_visible():
                                await email_field.fill(identity.email)
                                log.info("signup.step1_email  filled=%s", identity.email)
                        except Exception:
                            pass

                        try:
                            pass_field = page.locator('input[type="password"]').first
                            if await pass_field.count() > 0 and await pass_field.is_visible():
                                await pass_field.fill(identity.password)
                                log.info("signup.step1_password  filled")
                        except Exception:
                            pass

                        # Check any checkboxes on this step too
                        try:
                            step1_boxes = page.locator('input[type="checkbox"]')
                            for i in range(await step1_boxes.count()):
                                box = step1_boxes.nth(i)
                                if await box.is_visible() and not await box.is_checked():
                                    await box.check()
                                    log.info("signup.step1_checkbox_checked")
                        except Exception:
                            pass

                        # Submit step 1
                        for btn_text in ["Sign up", "Register", "Create", "Submit", "Next", "Continue"]:
                            try:
                                btn = page.get_by_role("button", name=re.compile(btn_text, re.IGNORECASE)).first
                                if await btn.count() > 0 and await btn.is_visible():
                                    await btn.click()
                                    await page.wait_for_timeout(5000)
                                    log.info("signup.step1_submitted  button=%s  url=%s", btn_text, page.url)
                                    break
                            except Exception:
                                pass

                        # Now on step 2 — business details form should be visible
                        log.info("signup.step2  url=%s", page.url)

                    # Check for modals/overlays — dismiss cookie banners and focus signup modals
                    for dismiss_text in ["Accept", "Accept All", "Got it", "I Accept", "OK", "Dismiss", "Close", "No thanks"]:
                        try:
                            dismiss_btn = page.get_by_role("button", name=re.compile(dismiss_text, re.IGNORECASE)).first
                            if await dismiss_btn.count() > 0 and await dismiss_btn.is_visible():
                                await dismiss_btn.click()
                                await page.wait_for_timeout(1000)
                                log.info("signup.dismissed_popup  text=%s", dismiss_text)
                                break
                        except Exception:
                            pass

                    # If a modal is covering the form, interact with it
                    for modal_sel in ['[role="dialog"]', '.modal.show', '.modal[style*="display: block"]', '[class*="overlay"][class*="visible"]']:
                        try:
                            modal = page.locator(modal_sel).first
                            if await modal.count() > 0 and await modal.is_visible():
                                # Try to close the modal first (might be cookie/promo popup)
                                close_btn = modal.locator('button[class*="close"], [aria-label="Close"], .close').first
                                if await close_btn.count() > 0:
                                    await close_btn.click()
                                    await page.wait_for_timeout(1000)
                                    log.info("signup.closed_modal  selector=%s", modal_sel)
                                else:
                                    log.info("signup.modal_detected  selector=%s", modal_sel)
                                break
                        except Exception:
                            pass

                # ---- Smart field filling: analyze each field individually ----
                first_name = identity.contact_name.split()[0] if identity.contact_name else ""
                last_name = identity.contact_name.split()[-1] if identity.contact_name else ""

                # Get all visible input/select/textarea with their attributes
                all_fields = await page.evaluate('''() => {
                    const els = document.querySelectorAll('input, select, textarea');
                    return Array.from(els).filter(el => el.offsetParent !== null).map((el, idx) => ({
                        idx: idx,
                        tag: el.tagName.toLowerCase(),
                        type: (el.type || '').toLowerCase(),
                        name: (el.name || '').toLowerCase(),
                        id: (el.id || '').toLowerCase(),
                        placeholder: (el.placeholder || '').toLowerCase(),
                        label: el.labels && el.labels[0] ? el.labels[0].innerText.trim().toLowerCase() : '',
                        ariaLabel: (el.getAttribute('aria-label') || '').toLowerCase(),
                    }))
                }''')

                filled = 0
                filled_fields = set()  # Track which fields we've filled to avoid duplicates

                for field in all_fields:
                    fid = f"{field['tag']}_{field['idx']}"
                    if fid in filled_fields:
                        continue

                    # Build a combined signature for matching
                    sig = f"{field['name']} {field['id']} {field['placeholder']} {field['label']} {field['ariaLabel']}"

                    # Determine what value this field needs
                    value = None

                    if field['type'] == 'hidden' or field['type'] == 'submit':
                        continue

                    # ---- SKIP rules first (before any matching) ----
                    if any(k in sig for k in ['fax']):
                        continue
                    if any(k in sig for k in ['twitter', 'facebook', 'instagram', 'linkedin', 'youtube', 'tiktok', 'pinterest', 'social']):
                        continue
                    if any(k in sig for k in ['messenger', 'msn', 'skype', 'whatsapp', 'video calling']):
                        continue
                    if field['name'] == 'im' or field['name'] == 'blog' or field['name'] == 'display_website':
                        continue
                    if any(k in sig for k in ['address2', 'address_2', 'apt', 'suite', 'unit']):
                        continue

                    # ---- Category / select (BEFORE other matches — custom dropdowns) ----
                    if any(k in sig for k in ['select category', 'category', 'industry', 'type of business']):
                        value = identity.category or "Lighting"
                    # Email field (MUST check before name/address)
                    elif field['type'] == 'email' or (field['name'] == 'email') or (field['id'] == 'email'):
                        value = identity.email
                    elif 'email' in sig and 'address' not in field['name'] and 'business' not in sig:
                        value = identity.email
                    # Password
                    elif field['type'] == 'password':
                        value = identity.password
                    # Phone
                    elif field['type'] == 'tel' or field['name'] == 'phone' or field['name'] == 'mobile':
                        value = identity.phone
                    elif 'phone' in sig and 'fax' not in sig:
                        value = identity.phone
                    # Address / Street (check BEFORE business — "business address" = address, not business name)
                    elif field['name'] == 'address' or field['tag'] == 'textarea' and 'address' in sig:
                        value = identity.address
                    elif any(k in sig for k in ['street', 'address1', 'address_1']):
                        value = identity.address
                    # Website (only exact website fields)
                    elif field['type'] == 'url' or field['name'] == 'website':
                        value = identity.website
                    elif 'website' in sig and 'display' not in sig:
                        value = identity.website
                    # First name
                    elif any(k in sig for k in ['first_name', 'first-name', 'firstname', 'first name', 'given']):
                        value = first_name
                    # Last name
                    elif any(k in sig for k in ['last_name', 'last-name', 'lastname', 'last name', 'surname', 'family']):
                        value = last_name
                    # Business / Company name
                    elif field['name'] in ('company', 'business', 'business_name', 'company_name', 'organization'):
                        value = identity.business_name
                    elif any(k in sig for k in ['company', 'organization', 'firm']):
                        value = identity.business_name
                    # "business name" in placeholder (but NOT "business address")
                    elif 'business' in sig and 'name' in sig and 'address' not in sig:
                        value = identity.business_name
                    # Generic "name" field
                    elif field['name'] == 'name' or ('name' in sig and not any(k in sig for k in ['user', 'domain', 'first', 'last'])):
                        value = identity.business_name
                    # City / Location
                    elif field['name'] == 'city' or 'city' in sig:
                        value = identity.city
                    # Postal / ZIP
                    elif any(k in sig for k in ['postal', 'zip', 'postcode']):
                        value = "V1Y 1A1"
                    # State / Province
                    elif any(k in sig for k in ['state', 'province', 'region']):
                        value = "British Columbia"
                    # Country
                    elif 'country' in sig:
                        value = "Canada"
                    # Category / Industry
                    elif any(k in sig for k in ['category', 'industry', 'type of business', 'service']):
                        value = identity.category
                    # Description / About
                    elif field['tag'] == 'textarea' or any(k in sig for k in ['description', 'about', 'bio', 'detail']):
                        value = identity.description[:500]

                    if not value:
                        continue

                    try:
                        # Build a unique selector for this specific field
                        if field['id']:
                            sel = f"#{field['id']}"
                        elif field['name']:
                            sel = f"{field['tag']}[name='{field['name']}']"
                        elif field['placeholder']:
                            sel = f"{field['tag']}[placeholder='{field['placeholder']}']"
                        else:
                            sel = f"{field['tag']}:nth-of-type({field['idx'] + 1})"

                        el = page.locator(sel).first
                        if await el.count() > 0 and await el.is_visible():
                            # Handle SELECT (dropdown) vs INPUT
                            if field['tag'] == 'select':
                                # Try to select by value text
                                try:
                                    await el.select_option(label=value)
                                except Exception:
                                    try:
                                        # Try partial match
                                        options = await el.evaluate('''(el) => Array.from(el.options).map(o => o.text)''')
                                        for opt in options:
                                            if value.lower() in opt.lower() or opt.lower() in value.lower():
                                                await el.select_option(label=opt)
                                                break
                                    except Exception:
                                        pass
                            else:
                                await el.fill(value)

                            filled += 1
                            filled_fields.add(fid)
                            log.info("signup.field_filled  sig=%s  value=%s", sig[:40], str(value)[:30])
                    except Exception as e:
                        log.debug("signup.field_skip  sig=%s  err=%s", sig[:40], e)

                # Fill any remaining textareas not yet filled
                try:
                    textareas = page.locator("textarea")
                    for i in range(await textareas.count()):
                        ta = textareas.nth(i)
                        if await ta.is_visible():
                            current = await ta.input_value()
                            if not current:
                                await ta.fill(identity.description[:500])
                                filled += 1
                except Exception:
                    pass

                log.info("signup.total_filled  count=%d", filled)

                # ---- Handle custom dropdowns (click → search → select) ----
                # Only handle fields that weren't already filled by the smart filler
                already_filled_sigs = set()
                for f in all_fields:
                    fid = f"{f['tag']}_{f['idx']}"
                    if fid in filled_fields:
                        already_filled_sigs.add(f"{f['name']} {f['id']} {f['placeholder']}")

                # Use SHORT search terms — many sites only have broad categories
                category_search = identity.category.split()[0] if identity.category else "Lighting"
                # e.g. "Permanent Lighting" → "Permanent", but "Lighting" is better as a broad search
                if category_search.lower() in ("permanent", "residential", "commercial", "professional"):
                    category_search = "Lighting"  # Use the general term

                custom_dropdown_values = [
                    {"keywords": ["category", "industry", "business type"], "value": category_search},
                    {"keywords": ["country", "nation"], "value": "Canada"},
                    {"keywords": ["province", "state", "region"], "value": "British Columbia"},
                ]
                try:
                    # Find elements that look like custom pickers
                    picker_sels = [
                        'input[placeholder*="select" i]',
                        'input[placeholder*="choose" i]',
                        'button:has-text("Select")',
                        '[role="combobox"]',
                    ]
                    for p_sel in picker_sels:
                        try:
                            pickers = page.locator(p_sel)
                            for pi in range(await pickers.count()):
                                picker = pickers.nth(pi)
                                if not await picker.is_visible():
                                    continue
                                picker_text = (await picker.inner_text() if await picker.evaluate("el => el.tagName") != "INPUT"
                                               else await picker.get_attribute("placeholder") or "").lower()

                                # Match to a value
                                target_value = None
                                for dd_def in custom_dropdown_values:
                                    if any(k in picker_text for k in dd_def["keywords"]):
                                        target_value = dd_def["value"]
                                        break
                                if not target_value:
                                    continue

                                # Don't skip custom pickers — even if the text input was filled,
                                # react-select components need the option to be CLICKED from dropdown
                                # (filling the input text alone doesn't set the underlying value)

                                log.info("signup.custom_picker  text=%s  target=%s", picker_text[:30], target_value)

                                # Click to open — with crash protection
                                try:
                                    await picker.click()
                                    await page.wait_for_timeout(1000)
                                    # Verify page is still alive
                                    _ = await page.title()
                                except Exception as e:
                                    log.warning("signup.custom_picker_crash  text=%s  err=%s", picker_text[:30], str(e)[:50])
                                    break  # Page died, stop trying pickers

                                # Look for a search input that appeared inside the dropdown
                                search_input = page.locator(
                                    'input[placeholder*="search" i], input[placeholder*="type" i], '
                                    'input[placeholder*="filter" i]'
                                ).first
                                if await search_input.count() > 0 and await search_input.is_visible():
                                    await search_input.fill(target_value)
                                    await page.wait_for_timeout(2000)
                                    log.info("signup.custom_picker_searched  value=%s", target_value)
                                else:
                                    # No search box — try typing directly
                                    await picker.type(target_value, delay=50)
                                    await page.wait_for_timeout(1500)

                                # Click first matching suggestion
                                clicked_opt = False
                                option = page.get_by_text(target_value, exact=False).first
                                # Skip if the match is the picker itself or a label
                                for attempt in range(5):
                                    candidate = page.get_by_text(target_value, exact=False).nth(attempt)
                                    if await candidate.count() == 0:
                                        break
                                    tag = await candidate.evaluate("el => el.tagName")
                                    parent_class = await candidate.evaluate("el => el.parentElement ? el.parentElement.className : ''")
                                    # Accept if it's inside a dropdown menu or list
                                    if any(k in parent_class.lower() for k in ["option", "menu", "list", "result", "dropdown", "item", "popover"]):
                                        await candidate.click()
                                        clicked_opt = True
                                        log.info("signup.custom_picker_selected  value=%s  attempt=%d", target_value, attempt)
                                        await page.wait_for_timeout(500)
                                        break

                                if not clicked_opt:
                                    # Try role=option or li elements
                                    for opt_sel in [f'[role="option"]', 'li', '[class*="option"]']:
                                        try:
                                            opts = page.locator(opt_sel)
                                            for oi in range(await opts.count()):
                                                opt = opts.nth(oi)
                                                if await opt.is_visible():
                                                    opt_text = await opt.inner_text()
                                                    if target_value.lower() in opt_text.lower():
                                                        await opt.click()
                                                        clicked_opt = True
                                                        log.info("signup.custom_picker_selected  via=%s  text=%s", opt_sel, opt_text[:40])
                                                        await page.wait_for_timeout(500)
                                                        break
                                        except Exception:
                                            pass
                                        if clicked_opt:
                                            break

                                if not clicked_opt:
                                    # Press Escape to close dropdown, don't get stuck
                                    await page.keyboard.press("Escape")
                                    await page.wait_for_timeout(500)
                                    log.info("signup.custom_picker_no_match  value=%s  escaped", target_value)

                        except Exception as e:
                            log.debug("signup.custom_picker_err  sel=%s  err=%s", p_sel, e)
                except Exception as e:
                    log.debug("signup.custom_pickers_err  err=%s", e)

                # ---- Check ALL checkboxes (terms, agreements, newsletters, etc.) ----
                checked = 0
                try:
                    # Method 1: Find checkboxes by common patterns
                    checkbox_selectors = [
                        'input[type="checkbox"]',
                        'input[name*="agree" i]',
                        'input[name*="terms" i]',
                        'input[name*="accept" i]',
                        'input[name*="tos" i]',
                        'input[name*="privacy" i]',
                        'input[name*="consent" i]',
                        'input[id*="agree" i]',
                        'input[id*="terms" i]',
                        'input[id*="accept" i]',
                    ]
                    for sel in checkbox_selectors:
                        try:
                            boxes = page.locator(sel)
                            count = await boxes.count()
                            for i in range(count):
                                box = boxes.nth(i)
                                if await box.is_visible():
                                    is_checked = await box.is_checked()
                                    if not is_checked:
                                        await box.check()
                                        checked += 1
                                        log.info("signup.checkbox_checked  selector=%s  index=%d", sel, i)
                        except Exception:
                            pass

                    # Method 2: Click labels that contain agreement text
                    agreement_labels = [
                        "I agree", "I accept", "Terms", "agree to",
                        "accept the", "Privacy Policy", "terms of service",
                        "terms and conditions", "I have read",
                    ]
                    for label_text in agreement_labels:
                        try:
                            label = page.get_by_text(label_text, exact=False).first
                            if await label.count() > 0 and await label.is_visible():
                                # Check if it's a label wrapping a checkbox
                                tag = await label.evaluate("el => el.tagName")
                                if tag.upper() == "LABEL":
                                    await label.click()
                                    checked += 1
                                    log.info("signup.label_clicked  text=%s", label_text)
                        except Exception:
                            pass

                    # Method 3: Click any role=checkbox elements
                    try:
                        role_boxes = page.get_by_role("checkbox")
                        rcount = await role_boxes.count()
                        for i in range(rcount):
                            box = role_boxes.nth(i)
                            if await box.is_visible():
                                is_checked = await box.is_checked()
                                if not is_checked:
                                    await box.check()
                                    checked += 1
                    except Exception:
                        pass

                    log.info("signup.checkboxes_total  checked=%d", checked)
                except Exception as e:
                    log.warning("signup.checkbox_error  err=%s", e)

                # ---- Select dropdowns (country, state/province, category) ----
                try:
                    # Country dropdown
                    for sel in ['select[name*="country" i]', 'select[id*="country" i]']:
                        try:
                            dropdown = page.locator(sel).first
                            if await dropdown.count() > 0 and await dropdown.is_visible():
                                await dropdown.select_option(label="Canada")
                                log.info("signup.selected_country  Canada")
                        except Exception:
                            pass

                    # Province/state dropdown
                    for sel in ['select[name*="province" i]', 'select[name*="state" i]', 'select[id*="province" i]', 'select[id*="state" i]', 'select[name*="region" i]']:
                        try:
                            dropdown = page.locator(sel).first
                            if await dropdown.count() > 0 and await dropdown.is_visible():
                                try:
                                    await dropdown.select_option(label="British Columbia")
                                except Exception:
                                    await dropdown.select_option(label="BC")
                                log.info("signup.selected_province  BC")
                        except Exception:
                            pass
                except Exception:
                    pass

                # ---- CAPTCHA Detection + Solving ----
                captcha_result = await captcha_solver.detect_and_solve(page)
                log.info("signup.captcha  type=%s  solved=%s", captcha_result["type"], captcha_result.get("solved"))

                # Try to submit the form
                submitted = False
                for submit_text in ["Submit", "Sign Up", "Register", "Create", "Continue", "Next"]:
                    try:
                        submit_btn = page.get_by_role("button", name=re.compile(submit_text, re.IGNORECASE)).first
                        if await submit_btn.count() > 0 and await submit_btn.is_visible():
                            await submit_btn.click()
                            submitted = True
                            await page.wait_for_timeout(3000)
                            log.info("signup.submitted  button=%s", submit_text)
                            break
                    except Exception:
                        pass

                if not submitted:
                    # Try generic submit button
                    try:
                        submit = page.locator('button[type="submit"], input[type="submit"]').first
                        if await submit.count() > 0:
                            await submit.click()
                            submitted = True
                            await page.wait_for_timeout(3000)
                    except Exception:
                        pass

                title = await page.title()
                final_url = page.url

                # Wait for post-submit redirects
                await page.wait_for_timeout(5000)

                # ---- AUTO-DIAGNOSE: screenshot + check for errors after submit ----
                import os
                os.makedirs("debug_screenshots", exist_ok=True)
                safe_name = re.sub(r'[^a-zA-Z0-9]', '_', url)[:40]
                await page.screenshot(path=f"debug_screenshots/{safe_name}_after_submit.png")

                # Check for validation errors on page
                try:
                    page_body = (await page.inner_text("body"))[:3000].lower()
                    error_phrases = ["required", "error", "invalid", "please enter", "missing", "cannot be blank", "try again"]
                    found_errors = []
                    for line in (await page.inner_text("body")).split("\n"):
                        line = line.strip()
                        if line and len(line) > 3 and len(line) < 100:
                            if any(ep in line.lower() for ep in error_phrases):
                                found_errors.append(line)
                    if found_errors:
                        log.warning("signup.page_errors  errors=%s", found_errors[:5])
                    else:
                        log.info("signup.no_errors_on_page")
                except Exception:
                    pass

                # ---- STEP 2 DETECTION: If new form fields appeared, fill them ----
                try:
                    step2_fields = await page.evaluate("""() => {
                        return Array.from(document.querySelectorAll('input, textarea')).filter(
                            el => el.offsetParent !== null && !el.value && el.type !== 'hidden' && el.type !== 'submit'
                        ).map(el => ({
                            type: (el.type || '').toLowerCase(),
                            name: (el.name || '').toLowerCase(),
                            id: (el.id || '').toLowerCase(),
                            placeholder: (el.placeholder || '').toLowerCase(),
                        }))
                    }""")

                    if step2_fields and len(step2_fields) >= 2:
                        log.info("signup.step2_detected  new_fields=%d", len(step2_fields))

                        for f2 in step2_fields:
                            s2 = f"{f2['name']} {f2['id']} {f2['placeholder']}"
                            val = None

                            if f2['type'] == 'password':
                                val = identity.password
                            elif 'name' in s2 and 'user' not in s2:
                                val = identity.contact_name
                            elif f2['type'] == 'email' or 'email' in s2:
                                val = identity.email

                            if val:
                                sel2 = None
                                if f2['id']:
                                    sel2 = f"#{f2['id']}"
                                elif f2['name']:
                                    sel2 = f"input[name='{f2['name']}']"
                                elif f2['placeholder']:
                                    sel2 = f"input[placeholder='{f2['placeholder']}']"

                                if sel2:
                                    try:
                                        el2 = page.locator(sel2).first
                                        if await el2.count() > 0 and await el2.is_visible():
                                            await el2.fill(val)
                                            log.info("signup.step2_filled  field=%s  value=%s", s2[:30], str(val)[:20])
                                    except Exception:
                                        pass

                        # Fill ALL email fields that are empty
                        try:
                            email_fields = page.locator('input[type="email"], input[placeholder*="email" i], input[name*="email" i]')
                            for i in range(await email_fields.count()):
                                ef = email_fields.nth(i)
                                if await ef.is_visible() and not await ef.input_value():
                                    await ef.fill(identity.email)
                                    log.info("signup.step2_email  index=%d", i)
                        except Exception:
                            pass

                        # Fill ALL password fields (including confirm)
                        try:
                            pw_fields = page.locator('input[type="password"]')
                            for i in range(await pw_fields.count()):
                                pf = pw_fields.nth(i)
                                if await pf.is_visible() and not await pf.input_value():
                                    await pf.fill(identity.password)
                                    log.info("signup.step2_password  index=%d", i)
                        except Exception:
                            pass

                        # Check checkboxes (terms)
                        try:
                            s2_boxes = page.locator('input[type="checkbox"]')
                            for i in range(await s2_boxes.count()):
                                box = s2_boxes.nth(i)
                                if await box.is_visible() and not await box.is_checked():
                                    await box.check()
                                    log.info("signup.step2_checkbox  index=%d", i)
                        except Exception:
                            pass

                        # Handle reCAPTCHA on step 2
                        # Wait for reCAPTCHA iframe to fully load
                        await page.wait_for_timeout(5000)
                        recaptcha_solved = False

                        # Find all iframes on page
                        all_frames = page.frames
                        recaptcha_iframe = None
                        for frame in all_frames:
                            if "recaptcha" in frame.url:
                                recaptcha_iframe = frame
                                log.info("signup.step2_recaptcha_iframe  url=%s", frame.url[:80])
                                break

                        if recaptcha_iframe:
                            try:
                                # Click the "I'm not a robot" checkbox inside the iframe
                                # Use frame object directly + force click to bypass stability checks
                                checkbox = recaptcha_iframe.locator("#recaptcha-anchor")
                                log.info("signup.step2_clicking_recaptcha...")
                                try:
                                    await checkbox.click(timeout=5000, force=True)
                                except Exception:
                                    # If locator click fails, try clicking via JS evaluation
                                    try:
                                        await recaptcha_iframe.evaluate('document.querySelector("#recaptcha-anchor").click()')
                                    except Exception:
                                        # Last resort: click by coordinates on the iframe element
                                        iframe_el = page.locator('iframe[src*="recaptcha"]').first
                                        if await iframe_el.count() > 0:
                                            box = await iframe_el.bounding_box()
                                            if box:
                                                # Click roughly where the checkbox is (left side, middle height)
                                                await page.mouse.click(box["x"] + 30, box["y"] + box["height"] / 2)
                                                log.info("signup.step2_recaptcha_clicked_by_coords")
                                log.info("signup.step2_recaptcha_clicked  waiting for solve...")

                                # Wait up to 30 seconds for the green checkmark
                                for wait_sec in range(30):
                                    await page.wait_for_timeout(1000)
                                    try:
                                        checked = await recaptcha_iframe.locator(".recaptcha-checkbox-checked").count()
                                        if checked > 0:
                                            recaptcha_solved = True
                                            log.info("signup.step2_recaptcha_SOLVED  waited=%ds", wait_sec + 1)
                                            break
                                        # Also check aria-checked attribute
                                        aria = await checkbox.get_attribute("aria-checked")
                                        if aria == "true":
                                            recaptcha_solved = True
                                            log.info("signup.step2_recaptcha_SOLVED_aria  waited=%ds", wait_sec + 1)
                                            break
                                    except Exception:
                                        pass
                                    if wait_sec % 5 == 4:
                                        log.info("signup.step2_recaptcha_waiting  %ds...", wait_sec + 1)

                                if not recaptcha_solved:
                                    log.warning("signup.step2_recaptcha_NOT_solved  waited 30s  trying submit anyway")
                                    await page.screenshot(path=f"debug_screenshots/{safe_name}_recaptcha_fail.png")
                                    # Try submitting anyway — some sites accept without captcha
                                    for btn_text in ["Next", "Submit", "Register", "Create", "Continue"]:
                                        try:
                                            btn = page.get_by_role("button", name=re.compile(btn_text, re.IGNORECASE)).first
                                            if await btn.count() > 0 and await btn.is_visible():
                                                await btn.click()
                                                await page.wait_for_timeout(5000)
                                                log.info("signup.step2_submitted_without_captcha  button=%s", btn_text)
                                                await page.screenshot(path=f"debug_screenshots/{safe_name}_after_nocaptcha_submit.png")
                                                break
                                        except Exception:
                                            pass
                            except Exception as e:
                                log.warning("signup.step2_recaptcha_click_err  err=%s", str(e)[:100])
                        else:
                            # No recaptcha iframe found — try general solver
                            captcha2 = await captcha_solver.detect_and_solve(page)
                            if captcha2.get("solved"):
                                recaptcha_solved = True
                            if captcha2.get("type") != "none":
                                log.info("signup.step2_captcha  type=%s  solved=%s", captcha2["type"], captcha2.get("solved"))

                        # Submit step 2
                        for btn_text in ["Next", "Submit", "Register", "Create", "Continue", "Finish", "Complete"]:
                            try:
                                btn = page.get_by_role("button", name=re.compile(btn_text, re.IGNORECASE)).first
                                if await btn.count() > 0 and await btn.is_visible():
                                    await btn.click()
                                    await page.wait_for_timeout(5000)
                                    log.info("signup.step2_submitted  button=%s", btn_text)
                                    break
                            except Exception:
                                pass

                        # Screenshot after step 2
                        await page.screenshot(path=f"debug_screenshots/{safe_name}_step2.png")
                except Exception as e:
                    log.debug("signup.step2_err  err=%s", e)

                # ---- CRITICAL: Detect verification code page ----
                # Some sites (Foursquare, etc.) ask "enter the code we sent you"
                page_text = ""
                try:
                    page_text = (await page.inner_text("body"))[:2000].lower()
                except Exception:
                    pass

                code_page = any(phrase in page_text for phrase in [
                    "enter the code", "verification code", "enter code",
                    "we sent you a code", "check your email", "confirm your email",
                    "enter the verification", "one-time code", "otp",
                    "we sent a code", "we just sent", "code we sent",
                    "enter your code", "6-digit code", "4-digit code",
                ])

                entered_code = False
                if code_page:
                    log.info("signup.code_page_detected  checking email for code...")

                    # Check email for verification code (quick poll — 30 seconds)
                    for _ in range(6):
                        inbox = await self._check_inbox()
                        if inbox:
                            body = inbox.get("body", "")
                            # Find verification codes (4-8 digit numbers)
                            codes = re.findall(r'\b(\d{4,8})\b', body)
                            if codes:
                                code = codes[0]
                                log.info("signup.got_code  code=%s", code)

                                # Find the code input field and enter it
                                code_selectors = [
                                    'input[name*="code" i]', 'input[name*="otp" i]',
                                    'input[name*="token" i]', 'input[name*="verification" i]',
                                    'input[id*="code" i]', 'input[id*="otp" i]',
                                    'input[placeholder*="code" i]', 'input[placeholder*="enter" i]',
                                    'input[type="number"]', 'input[type="tel"]',
                                    'input[maxlength="6"]', 'input[maxlength="4"]',
                                    'input:not([type="hidden"]):not([type="submit"])',
                                ]
                                for sel in code_selectors:
                                    try:
                                        code_input = page.locator(sel).first
                                        if await code_input.count() > 0 and await code_input.is_visible():
                                            await code_input.fill(code)
                                            log.info("signup.code_entered  selector=%s", sel)

                                            # Click verify/submit
                                            for vbtn in ["Verify", "Confirm", "Submit", "Continue", "Next"]:
                                                try:
                                                    btn = page.get_by_role("button", name=re.compile(vbtn, re.IGNORECASE)).first
                                                    if await btn.count() > 0 and await btn.is_visible():
                                                        await btn.click()
                                                        await page.wait_for_timeout(3000)
                                                        entered_code = True
                                                        log.info("signup.code_submitted  button=%s", vbtn)
                                                        break
                                                except Exception:
                                                    pass
                                            break
                                    except Exception:
                                        pass
                                if entered_code:
                                    break
                        await asyncio.sleep(5)

                    if not entered_code:
                        log.warning("signup.code_not_found  no code in email within 30s")

                title = await page.title()
                final_url = page.url
                try:
                    if use_camoufox:
                        await self._camoufox_ctx.__aexit__(None, None, None)
                    else:
                        await browser.close()
                except Exception:
                    pass

            return {
                "status": "filled",
                "fields_filled": filled,
                "form_submitted": submitted,
                "code_entered": entered_code,
                "captcha": captcha_result,
                "page_title": title,
                "final_url": final_url,
            }

        except Exception as e:
            log.error("signup.playwright_fail  url=%s  err=%s", url, e)
            return {"status": "failed", "error": str(e)}

    # ---- Step 4: Check for confirmation email ----

    async def wait_for_confirmation(self, timeout_seconds: int = 120) -> dict | None:
        """Poll inbox for confirmation email. Supports pymailtm, mail.tm API, and guerrilla."""
        log.info("signup.waiting_for_email  timeout=%ds", timeout_seconds)
        start = time.time()

        while time.time() - start < timeout_seconds:
            try:
                messages_data = await self._check_inbox()
                if messages_data:
                    body = messages_data.get("body", "")
                    subject = messages_data.get("subject", "")

                    # Extract verification links (broad patterns)
                    links = re.findall(
                        r'https?://[^\s<>"\']+(?:verify|confirm|activate|token|validate|click|registration|email)[^\s<>"\']*',
                        body, re.IGNORECASE
                    )
                    # Extract verification codes
                    codes = re.findall(r'\b(\d{4,8})\b', body)

                    result = {
                        "subject": subject,
                        "from": messages_data.get("from", ""),
                        "verification_links": links[:5],
                        "verification_codes": codes[:3],
                        "body_preview": body[:500],
                    }
                    log.info("signup.email_received  subject=%s  links=%d  codes=%d",
                             subject, len(links), len(codes))
                    return result
            except Exception as e:
                log.debug("signup.poll_error  err=%s", e)

            await asyncio.sleep(5)

        log.warning("signup.email_timeout  waited=%ds", timeout_seconds)
        return None

    async def _check_inbox(self) -> dict | None:
        """Check inbox. Try pymailtm first (direct account access), then API, then guerrilla."""
        # Method 1: pymailtm account object (MOST RELIABLE when available)
        if self._email_account:
            try:
                messages = self._email_account.get_messages()
                if messages:
                    msg = messages[0]
                    body = getattr(msg, "text", "") or getattr(msg, "html", "") or ""
                    if body:
                        return {
                            "subject": getattr(msg, "subject", ""),
                            "from": str(getattr(msg, "from_", "")),
                            "body": body,
                        }
            except Exception:
                pass

        # Method 2: mail.tm API with stored session (MOST RELIABLE)
        s = getattr(self, "_mail_tm_session", None)
        token = getattr(self, "_mail_tm_token", "")
        if s and token:
            try:
                resp = s.get(
                    "https://api.mail.tm/messages",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10,
                )
                if resp.status_code == 200:
                    msgs = resp.json().get("hydra:member", [])
                    if msgs:
                        msg_id = msgs[0]["id"]
                        detail = s.get(
                            f"https://api.mail.tm/messages/{msg_id}",
                            headers={"Authorization": f"Bearer {token}"},
                            timeout=10,
                        )
                        d = detail.json()
                        body = d.get("text", "")
                        if not body and d.get("html"):
                            body = d["html"][0] if isinstance(d["html"], list) else d["html"]
                        return {
                            "subject": d.get("subject", ""),
                            "from": str(d.get("from", {}).get("address", "")),
                            "body": body,
                        }
                elif resp.status_code == 401:
                    # Re-auth with same session
                    pw = getattr(self, "_mail_tm_password", "")
                    addr = getattr(self, "_mail_tm_email", "")
                    if pw and addr:
                        tr = s.post("https://api.mail.tm/token", json={"address": addr, "password": pw}, timeout=10)
                        if tr.status_code == 200:
                            self._mail_tm_token = tr.json().get("token", "")
                            # Retry
                            resp2 = s.get("https://api.mail.tm/messages", headers={"Authorization": f"Bearer {self._mail_tm_token}"}, timeout=10)
                            if resp2.status_code == 200:
                                msgs = resp2.json().get("hydra:member", [])
                                if msgs:
                                    msg_id = msgs[0]["id"]
                                    detail = s.get(f"https://api.mail.tm/messages/{msg_id}", headers={"Authorization": f"Bearer {self._mail_tm_token}"}, timeout=10)
                                    d = detail.json()
                                    body = d.get("text", "")
                                    if not body and d.get("html"):
                                        body = d["html"][0] if isinstance(d["html"], list) else d["html"]
                                    return {"subject": d.get("subject", ""), "from": str(d.get("from", {}).get("address", "")), "body": body}
            except Exception:
                pass

        # Method 3: Guerrilla Mail
        sid = getattr(self, "_guerrilla_sid", "")
        if sid:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(
                        f"https://api.guerrillamail.com/ajax.php?f=check_email&sid_token={sid}&seq=0"
                    )
                    emails = resp.json().get("list", [])
                    if emails and emails[0].get("mail_subject"):
                        e = emails[0]
                        # Get full email body
                        body_resp = await client.get(
                            f"https://api.guerrillamail.com/ajax.php?f=fetch_email&sid_token={sid}&email_id={e['mail_id']}"
                        )
                        body_data = body_resp.json()
                        return {
                            "subject": e.get("mail_subject", ""),
                            "from": e.get("mail_from", ""),
                            "body": body_data.get("mail_body", ""),
                        }
            except Exception:
                pass

        return None

    # ---- Step 5: Complete verification ----

    async def verify(self, confirmation: dict) -> dict:
        """Click verification link or enter code."""
        links = confirmation.get("verification_links", [])
        if not links:
            return {"status": "no_link", "codes": confirmation.get("verification_codes", [])}

        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(links[0], timeout=15000)
                await page.wait_for_timeout(3000)
                title = await page.title()
                try:
                    if use_camoufox:
                        await self._camoufox_ctx.__aexit__(None, None, None)
                    else:
                        await browser.close()
                except Exception:
                    pass

            log.info("signup.verified  link=%s  title=%s", links[0][:50], title[:30])
            return {"status": "verified", "link": links[0], "page_title": title}

        except Exception as e:
            log.error("signup.verify_fail  err=%s", e)
            return {"status": "failed", "error": str(e)}

    # ---- Full Pipeline ----

    async def auto_register(
        self,
        site_url: str,
        site_name: str,
        business_name: str,
        website: str = "",
        city: str = "",
        service: str = "",
        phone: str = "",
        description: str = "",
        use_ai: bool = False,
    ) -> SignupResult:
        """Full auto-signup pipeline for one site."""
        result = SignupResult(site=site_name, url=site_url)

        # Step 1: Create email
        email, password = await self.create_email()
        result.email_used = email
        result.status = "email_created"

        # Step 2: Create identity
        identity = self.create_identity(
            business_name=business_name,
            website=website,
            city=city,
            service=service,
            phone=phone,
            description=description,
            email=email,
        )
        identity.password = password

        # Step 3: Fill form
        if use_ai:
            form_result = await self.fill_form_ai(site_url, identity)
        else:
            form_result = await self.fill_form_playwright(site_url, identity)

        if form_result.get("status") == "filled":
            result.status = "form_filled"
        else:
            result.status = "failed"
            result.error = form_result.get("error", "Form fill failed")
            return result

        # Step 4: Wait for confirmation
        confirmation = await self.wait_for_confirmation(timeout_seconds=60)
        if confirmation:
            result.confirmation_received = True

            # Step 5: Verify
            verify_result = await self.verify(confirmation)
            if verify_result.get("status") == "verified":
                result.verification_completed = True
                result.status = "verified"
            else:
                result.status = "confirmation_received"
        else:
            result.status = "form_filled"  # No email yet, may arrive later

        log.info("signup.complete  site=%s  status=%s", site_name, result.status)
        return result
