"""
EU -> UK Amazon Arbitrage Monitor
==================================
Scans Amazon FR / DE / IT / ES for deals (via Keepa's Deals feed), matches
each deal to its UK ASIN via EAN/UPC/GTIN, pulls current UK pricing, runs
your profitability filter, and posts qualifying leads to Discord.

Uses the official `keepa` Python library (https://keepaapi.readthedocs.io)
rather than raw REST calls - it handles the token bucket automatically
(blocks and waits for tokens as needed) and returns correctly-parsed
dictionaries, so we don't have to hand-roll throttling or guess field names.

ENV VARS REQUIRED (set as GitHub Actions secrets):
    KEEPA_API_KEY
    DISCORD_WEBHOOK_URL

CONFIG:
    Edit the CONFIG block below to set source countries, categories,
    deal thresholds, VAT rate, referral fee %, FBA fee estimate, and
    your minimum profit/ROI thresholds.
"""

import os
import json
import time
import requests
import keepa
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote

# ---------------------------------------------------------------------------
# CONFIG — adjust to match your existing SAS/Keepa filter thresholds
# ---------------------------------------------------------------------------

KEEPA_API_KEY = os.environ["KEEPA_API_KEY"]
DISCORD_WEBHOOK_URL = os.environ["DISCORD_WEBHOOK_URL"]

if not DISCORD_WEBHOOK_URL or not DISCORD_WEBHOOK_URL.startswith("http"):
    raise SystemExit(
        "DISCORD_WEBHOOK_URL is empty or invalid. Check the repo secret "
        "(Settings -> Secrets and variables -> Actions) actually contains "
        "your current webhook URL, with no leading/trailing whitespace."
    )

api = keepa.Keepa(KEEPA_API_KEY, timeout=30)

DOMAIN_UK = "GB"
SOURCE_DOMAINS = {
    "France": "FR",
    "Germany": "DE",
    "Italy": "IT",
    "Spain": "ES",
}
DOMAIN_TLD = {"France": "fr", "Germany": "de", "Italy": "it", "Spain": "es"}

# Keepa's deals() endpoint requires domainId INSIDE the deal_parms dict
# itself (not just the separate domain= kwarg) - numeric IDs per Keepa docs.
DOMAIN_IDS = {"GB": 2, "DE": 3, "FR": 4, "IT": 8, "ES": 9}

# Deals feed filter — see https://keepaapi.readthedocs.io for full schema.
# priceTypes: 0 = Amazon, 1 = New (3rd party/FBA), 2 = Used, 18 = Warehouse
DEALS_PARAMS_TEMPLATE = {
    "page": 0,
    "priceTypes": [1],
    "deltaPercentRange": [15, 100],   # min 15% price drop
    "dateRange": 1,                    # 0 = day, 1 = 3 days, 2 = week
    "isRangeEnabled": True,
    "isFilterEnabled": True,
    "sortType": 4,                     # sort by biggest % drop
}

# Hard cap on how many deals get fully processed (product lookup + EAN
# cross-check + profit calc) per country per run - the real lever on token
# spend. Lower this if you're still hitting rate limits.
MAX_DEALS_PER_COUNTRY = 50

# Since Amazon applies UK VAT (OSS) on EU cross-border sales to your UK VAT
# number, treat the EU buy price like a domestic buy: strip VAT to get net
# cost (you reclaim it as input VAT), and account for output VAT on the UK
# sale side — same treatment as your domestic Amazon/eBay VAT tracker.
VAT_RATE = 0.20

# Rough fee assumptions — replace with real category-specific figures from
# SAS/Keepa fee data where possible. These are deliberately conservative.
REFERRAL_FEE_RATE = 0.15     # most categories; override per-category if needed
FBA_FEE_FLAT_ESTIMATE = 3.50 # £, small-standard placeholder — refine per ASIN/size tier

# Profitability thresholds — mirror your existing filters
MIN_PROFIT_GBP = 2.00
MIN_ROI_PCT = 20.0
MIN_MARGIN_PCT = 10.0

# Only alert if the UK listing has between this many active New sellers -
# too few (0-1) often means low demand or an Amazon-only listing; too many
# means a race-to-the-bottom price war that'll erode margin fast.
MIN_SELLERS = 2
MAX_SELLERS = 10

# Set to True for a one-off test run: fires a real Discord alert on the FIRST
# evaluated deal regardless of whether it passes the profit filters, then
# stops. Use this to confirm the whole pipeline works end-to-end without
# waiting for a genuine profitable match. Set back to False for real runs.
DEBUG_FORCE_ALERT = os.environ.get("DEBUG_FORCE_ALERT", "false").lower() == "true"

# FX buffer applied to EUR->GBP conversion to protect against rate swings
FX_BUFFER_PCT = 1.5

STATE_FILE = Path(__file__).parent / "seen_deals.json"

# ---------------------------------------------------------------------------
# STATE (dedupe so we don't re-alert the same EAN+country every run)
# ---------------------------------------------------------------------------

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))

# ---------------------------------------------------------------------------
# KEEPA CALLS (via the official library — wait=True handles token pacing)
# ---------------------------------------------------------------------------

def fetch_deals(domain):
    deal_parms = dict(DEALS_PARAMS_TEMPLATE)
    deal_parms["domainId"] = DOMAIN_IDS[domain]
    result = api.deals(deal_parms, domain=domain, wait=True)
    return result.get("dr", [])

def fetch_product_by_asin(asin, domain):
    products = api.query(asin, domain=domain, stats=90, offers=20,
                          history=False, wait=True, progress_bar=False)
    return products[0] if products else None

def fetch_products_by_code(code, domain):
    """Look up product(s) by EAN/UPC/GTIN on a given marketplace."""
    products = api.query(code, domain=domain, stats=90, history=False, offers=20,
                          product_code_is_asin=False, wait=True, progress_bar=False)
    return products

# ---------------------------------------------------------------------------
# PRICE HELPERS
# ---------------------------------------------------------------------------

def get_current_buybox_price(product):
    stats = product.get("stats") or {}
    current = stats.get("current") or {}
    if isinstance(current, dict):
        for key in ("BUY_BOX_SHIPPING", "NEW", "AMAZON"):
            val = current.get(key)
            if val is not None and val >= 0:
                return round(val / 100, 2) if val > 1000 else round(val, 2)
    elif isinstance(current, list):
        for idx in (18, 1):
            if len(current) > idx and current[idx] and current[idx] > 0:
                return round(current[idx] / 100, 2)
    return None

def get_avg_price(product):
    stats = product.get("stats") or {}
    avg = stats.get("avg90") or {}
    if isinstance(avg, dict):
        for key in ("BUY_BOX_SHIPPING", "NEW", "AMAZON"):
            val = avg.get(key)
            if val is not None and val >= 0:
                return round(val / 100, 2) if val > 1000 else round(val, 2)
    elif isinstance(avg, list):
        for idx in (18, 1):
            if len(avg) > idx and avg[idx] and avg[idx] > 0:
                return round(avg[idx] / 100, 2)
    return None

def get_primary_ean(product):
    eans = product.get("eanList") or []
    return eans[0] if eans else None

def get_seller_count(product):
    """Number of active New offers on the listing (competition check)."""
    stats = product.get("stats") or {}
    current = stats.get("current") or {}
    if isinstance(current, dict):
        count = current.get("COUNT_NEW")
        if count is not None and count >= 0:
            return int(count)
    offers = product.get("offers")
    if offers:
        return len(offers)
    return None

# ---------------------------------------------------------------------------
# FX (simple, cached per run)
# ---------------------------------------------------------------------------

_fx_cache = {}

def eur_to_gbp_rate():
    if "rate" in _fx_cache:
        return _fx_cache["rate"]
    try:
        r = requests.get("https://api.exchangerate.host/latest",
                          params={"base": "EUR", "symbols": "GBP"}, timeout=15)
        rate = r.json()["rates"]["GBP"]
    except Exception:
        rate = 0.86  # fallback static rate — update if API unavailable
    _fx_cache["rate"] = rate
    return rate

# ---------------------------------------------------------------------------
# PROFIT CALC
# ---------------------------------------------------------------------------

def calc_profit(buy_price_eur, sell_price_gbp):
    fx = eur_to_gbp_rate() * (1 - FX_BUFFER_PCT / 100)
    buy_gbp_gross = buy_price_eur * fx
    buy_gbp_net = buy_gbp_gross / (1 + VAT_RATE)   # reclaimable input VAT

    sell_net = sell_price_gbp / (1 + VAT_RATE)     # output VAT due to HMRC
    referral_fee = sell_net * REFERRAL_FEE_RATE
    fba_fee = FBA_FEE_FLAT_ESTIMATE

    profit = sell_net - buy_gbp_net - referral_fee - fba_fee
    roi_pct = (profit / buy_gbp_net * 100) if buy_gbp_net else 0
    margin_pct = (profit / sell_net * 100) if sell_net else 0

    return {
        "buy_gbp_net": round(buy_gbp_net, 2),
        "sell_gbp_gross": round(sell_price_gbp, 2),
        "referral_fee": round(referral_fee, 2),
        "fba_fee": round(fba_fee, 2),
        "profit": round(profit, 2),
        "roi_pct": round(roi_pct, 2),
        "margin_pct": round(margin_pct, 2),
        "fx_rate_used": round(fx, 4),
    }

# ---------------------------------------------------------------------------
# DISCORD ALERT
# ---------------------------------------------------------------------------

def sas_ean(ean, cost_inc):
    return f"https://sas.selleramp.com/sas/lookup/?search_term={ean}&sas_cost_price={cost_inc:.2f}"

def sas_title(title, cost_inc):
    return f"https://sas.selleramp.com/sas/lookup/?search_term={quote(title)}&sas_cost_price={cost_inc:.2f}"

def get_thumbnail_url(product):
    """Build an Amazon image CDN URL from Keepa's imagesCSV field."""
    images_csv = product.get("imagesCSV")
    if not images_csv:
        return None
    first_image = images_csv.split(",")[0].strip()
    if not first_image:
        return None
    return f"https://images-na.ssl-images-amazon.com/images/I/{first_image}"

def fetch_graph_image_bytes(asin, domain):
    """
    Download a Keepa price-history graph PNG via the library's built-in
    helper. Never link the raw graphimage URL in Discord - it carries your
    API key. This downloads to a temp file and reads the bytes back.
    """
    tmp_path = f"/tmp/graph_{asin}.png"
    try:
        api.download_graph_image(asin, tmp_path, wait=True)
        with open(tmp_path, "rb") as f:
            return f.read()
    except Exception as e:
        print(f"  [!] Graph image fetch failed for {asin}: {e}")
        return None

def send_discord_alert(country, source_product, uk_product, calc, ean, seller_count=None):
    title = source_product.get("title", "Unknown product")
    source_asin = source_product.get("asin")
    uk_asin = uk_product.get("asin")
    spacer = {"name": "\u200b", "value": "\u200b", "inline": True}

    embed = {
        "title": title[:250],
        "color": 3066993,
        "fields": [
            {"name": "Route", "value": f"{country} \u2192 UK", "inline": True},
            {"name": "EAN", "value": f"`{ean or 'n/a'}`", "inline": True},
            spacer,
            {"name": "Buy (net, GBP)", "value": f"£{calc['buy_gbp_net']}", "inline": True},
            {"name": "Sell (UK buy box)", "value": f"£{calc['sell_gbp_gross']}", "inline": True},
            {"name": "Profit", "value": f"£{calc['profit']}", "inline": True},
            {"name": "ROI", "value": f"{calc['roi_pct']}%", "inline": True},
            {"name": "Margin", "value": f"{calc['margin_pct']}%", "inline": True},
            {"name": "UK Sellers", "value": str(seller_count) if seller_count is not None else "?", "inline": True},
            spacer,
            {"name": "Source ASIN", "value": f"`{source_asin}`", "inline": True},
            {"name": "UK ASIN", "value": f"`{uk_asin}`", "inline": True},
            spacer,
            {"name": "Links", "value":
                f"[Source Listing](https://amazon.{DOMAIN_TLD[country]}/dp/{source_asin}) | "
                f"[UK Listing](https://amazon.co.uk/dp/{uk_asin}) | "
                f"[Keepa UK](https://keepa.com/#!product/2-{uk_asin})"
            },
            {"name": "SAS Title", "value": f"[Search by title]({sas_title(title, calc['buy_gbp_net'])})", "inline": True},
            {"name": "SAS EAN", "value": f"[Search by barcode]({sas_ean(ean, calc['buy_gbp_net'])})" if ean else "-", "inline": True},
        ],
        "footer": {"text": f"EU\u2192UK Monitor \u2022 {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M')}"},
    }

    thumb = get_thumbnail_url(source_product) or get_thumbnail_url(uk_product)
    if thumb:
        embed["thumbnail"] = {"url": thumb}

    files = None
    graph_bytes = fetch_graph_image_bytes(uk_asin, DOMAIN_UK)
    if graph_bytes:
        embed["image"] = {"url": "attachment://graph.png"}
        files = {"file": ("graph.png", graph_bytes, "image/png")}

    payload = {"embeds": [embed]}
    if files:
        requests.post(DISCORD_WEBHOOK_URL, data={"payload_json": json.dumps(payload)}, files=files, timeout=20)
    else:
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=15)

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def process_country(country, domain, state):
    """Returns True if DEBUG_FORCE_ALERT fired during this call."""
    print(f"[{country}] fetching deals...")
    deals = fetch_deals(domain)
    print(f"[{country}] {len(deals)} deals returned, processing up to {MAX_DEALS_PER_COUNTRY}")
    deals = deals[:MAX_DEALS_PER_COUNTRY]

    for deal in deals:
        asin = deal.get("asin")
        if not asin:
            continue

        source_product = fetch_product_by_asin(asin, domain)
        if not source_product:
            print(f"  skip: {asin} - no product data returned")
            continue

        ean = get_primary_ean(source_product)
        if not ean:
            print(f"  skip: {asin} - no EAN, can't cross-match to UK")
            continue

        state_key = f"{country}:{ean}"
        buy_price = get_current_buybox_price(source_product) or get_avg_price(source_product)
        if buy_price is None:
            print(f"  skip: {asin} - no usable price found")
            continue

        prev = state.get(state_key)
        if prev and abs(prev.get("buy_price", 0) - buy_price) < 0.5:
            continue

        uk_matches = fetch_products_by_code(ean, DOMAIN_UK)
        if not uk_matches:
            print(f"  skip: {asin} (EAN {ean}) - no UK match found")
            continue

        for uk_product in uk_matches:
            uk_sell_price = get_current_buybox_price(uk_product) or get_avg_price(uk_product)
            if uk_sell_price is None:
                continue

            calc = calc_profit(buy_price, uk_sell_price)
            seller_count = get_seller_count(uk_product)

            title_short = source_product.get("title", "")[:55]
            print(f"  eval: {title_short} | buy=£{calc['buy_gbp_net']} "
                  f"sell=£{calc['sell_gbp_gross']} profit=£{calc['profit']} "
                  f"roi={calc['roi_pct']}% margin={calc['margin_pct']}% "
                  f"sellers={seller_count}")

            sellers_ok = (seller_count is not None
                          and MIN_SELLERS <= seller_count <= MAX_SELLERS)

            passes = (calc["profit"] >= MIN_PROFIT_GBP
                      and calc["roi_pct"] >= MIN_ROI_PCT
                      and calc["margin_pct"] >= MIN_MARGIN_PCT
                      and sellers_ok)

            if passes or DEBUG_FORCE_ALERT:
                if not passes:
                    print("  [DEBUG_FORCE_ALERT] sending despite failing filters")
                print(f"  MATCH: {title_short} profit=£{calc['profit']} roi={calc['roi_pct']}% sellers={seller_count}")
                send_discord_alert(country, source_product, uk_product, calc, ean, seller_count)
                state[state_key] = {
                    "buy_price": buy_price,
                    "uk_asin": uk_product.get("asin"),
                    "last_alerted": datetime.now(timezone.utc).isoformat(),
                    "profit": calc["profit"],
                    "roi_pct": calc["roi_pct"],
                }
                if DEBUG_FORCE_ALERT:
                    return True

    return False


def main():
    state = load_state()
    for country, domain in SOURCE_DOMAINS.items():
        debug_fired = False
        try:
            debug_fired = process_country(country, domain, state)
        except Exception as e:
            print(f"[{country}] ERROR: {e}")
        save_state(state)
        if debug_fired:
            print("[DEBUG_FORCE_ALERT] test alert sent, stopping run early.")
            break
    save_state(state)


if __name__ == "__main__":
    main()
