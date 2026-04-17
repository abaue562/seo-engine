import json, logging, sqlite3
from typing import Dict, Optional
import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)

PLANS = {
    "free": {
        "price_monthly": 0,
        "sites": 1, "keywords": 1, "pages_per_month": 3,
        "locations": 1, "api_access": False, "white_label": False,
        "outreach_prospects": 0, "seats": 1,
    },
    "starter": {
        "price_monthly": 299,
        "sites": 1, "keywords": 100, "pages_per_month": 25,
        "locations": 1, "api_access": False, "white_label": False,
        "outreach_prospects": 50, "seats": 2,
    },
    "growth": {
        "price_monthly": 799,
        "sites": 3, "keywords": 500, "pages_per_month": 100,
        "locations": 10, "api_access": True, "white_label": False,
        "outreach_prospects": 200, "seats": 5,
    },
    "scale": {
        "price_monthly": 1999,
        "sites": 10, "keywords": 2000, "pages_per_month": 400,
        "locations": 50, "api_access": True, "white_label": False,
        "outreach_prospects": 1000, "seats": 15,
    },
    "enterprise": {
        "price_monthly": 0,  # custom
        "sites": 999, "keywords": 99999, "pages_per_month": 9999,
        "locations": 999, "api_access": True, "white_label": True,
        "outreach_prospects": 99999, "seats": 999,
    },
    "agency": {
        "price_monthly": 0,  # per-client pricing
        "sites": 999, "keywords": 99999, "pages_per_month": 9999,
        "locations": 999, "api_access": True, "white_label": True,
        "outreach_prospects": 99999, "seats": 999,
    },
}

OVERAGE_RATES = {
    "keywords": 0.20,        # $/keyword/mo
    "pages_per_month": 5.00, # $/page (mid-tier default)
    "outreach_prospects": 1.00,
}

def get_plan(plan_name: str) -> Dict:
    return PLANS.get(plan_name, PLANS["free"])

def get_tenant_plan(business_id: str) -> str:
    cached = _redis.get(f"billing:plan:{business_id}")
    if cached:
        return cached
    try:
        conn = sqlite3.connect("data/storage/seo_engine.db")
        row = conn.execute("SELECT plan FROM tenant_billing WHERE tenant_id=?", [business_id]).fetchone()
        conn.close()
        plan = row[0] if row else "free"
    except Exception:
        plan = "free"
    _redis.setex(f"billing:plan:{business_id}", 300, plan)
    return plan

def check_limit(business_id: str, resource: str, current_usage: int) -> Dict:
    plan_name = get_tenant_plan(business_id)
    plan = get_plan(plan_name)
    limit = plan.get(resource, 0)
    pct = round(current_usage / max(limit, 1) * 100, 1)
    over = max(0, current_usage - limit)
    overage_cost = round(over * OVERAGE_RATES.get(resource, 0), 2)
    return {
        "plan": plan_name, "resource": resource,
        "limit": limit, "used": current_usage,
        "pct_used": pct, "over_limit": over > 0,
        "overage_units": over, "overage_cost_usd": overage_cost,
        "warn": pct >= 80,
    }

def calculate_overages(business_id: str, usage: Dict[str, int]) -> Dict:
    results = {}
    total_overage = 0.0
    for resource, used in usage.items():
        result = check_limit(business_id, resource, used)
        results[resource] = result
        total_overage += result["overage_cost_usd"]
    return {"business_id": business_id, "resources": results, "total_overage_usd": round(total_overage, 2)}

def annual_price(plan_name: str) -> float:
    monthly = PLANS.get(plan_name, {}).get("price_monthly", 0)
    return round(monthly * 12 * 0.80, 2)  # 20% discount

def annual_savings(plan_name: str) -> float:
    monthly = PLANS.get(plan_name, {}).get("price_monthly", 0)
    return round(monthly * 12 * 0.20, 2)
