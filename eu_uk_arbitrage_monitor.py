"""
EU -> UK Amazon Arbitrage Monitor
==================================
Scans Amazon FR / DE / IT / ES for deals (via Keepa's Deals feed), matches
each deal to its UK ASIN via EAN/UPC/GTIN, pulls current UK pricing, runs
your profitability filter, and posts qualifying leads to Discord.

Mirrors the structure of your Very Cosmetics / Cherry Cosmetics monitors:
state-file dedupe + Discord embed alerts, designed to run on a GitHub
Actions cron.

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
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote

# ---------------------------------------------------------------------------
# CONFIG — adjust to match your existing SAS/Keepa filter thresholds
# ---------------------------------------------------------------------------

KEEPA_API_KEY = os.environ["KEEPA_API_KEY"]
DISCORD_WEBHOOK_URL = os.environ["DISCORD_WEBHOOK_URL"]

# Keepa domainId per marketplace
DOMAIN_UK = 2
SOURCE_DOMAINS = {
    "France": 4,
    "Germany": 3,
    "Italy": 8,
    "Spain": 9,
}

# Deals feed filter — see Keepa docs for full selection schema.
# priceTypes: 0 = Amazon price, 1 = New (3rd party/FBA), 2 = Used, 3 = Sales rank drop, 18 = Warehouse
# We default to "New" FBA-eligible price drops, min 15% drop, seen in last 24h.
DEALS_SELECTION_TEMPLATE = {
    "page": 0,
    "domainId": None,          # filled in per-country
    "priceTypes": [1],
    "deltaPercentRange": [15, 100],   # min 15% price drop
    "dateRange": 1,             # 0 = day, 1 = 3 days, 2 = week
    "isRangeEnabled": True,
    "isFilterEnabled": True,
    "isFBASupported": True,
    "sortType": 4,               # sort by biggest % drop
    "perPage": 50,
}

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
MIN_PROFIT_GBP = 3.00
MIN_ROI_PCT = 20.0
MIN_MARGIN_PCT = 10.0

# FX buffer applied to EUR->GBP conversion to protect against rate swings
FX_BUFFER_PCT = 1.5

STATE_FILE = Path(__file__).parent / "seen_deals.json"
KEEPA_BASE = "https://api.keepa.com"

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
# KEEPA CALLS
# ---------------------------------------------------------------------------

def keepa_get(path, params, retries=3):
    params = {**params, "key": KEEPA_API_KEY}
    for attempt in range(retries):
        r = requests.get(f"{KEEPA_BASE}/{path}", params=params, timeout=30)
        if r.status_code == 429:
            wait = 60 * (attempt + 1)
            print(f"  [429] Rate limited, waiting {wait}s before retry...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        data = r.json()
        # Keepa returns tokensLeft in every response - throttle proactively
        tokens_left = data.get("tokensLeft")
        if tokens_left is not None and tokens_left < 20:
            print(f"  [!] Low tokens ({tokens_left}), pausing 30s to let them refill...")
            time.sleep(30)
        return data
    raise requests.exceptions.HTTPError(f"429 persisted after {retries} retries on {path}")

def fetch_deals(domain_id):
    selection = dict(DEALS_SELECTION_TEMPLATE)
    selection["domainId"] = domain_id
    data = keepa_get("deal", {"selection": json.dumps(selection)})
    return data.get("deals", {}).get("dr", [])

def fetch_product_by_asin(asin, domain_id, stats_days=90):
    data = keepa_get("product", {
        "domain": domain_id,
        "asin": asin,
        "stats": stats_days,
        "offers": 20,
    })
    products = data.get("products", [])
    return products[0] if products else None

def fetch_products_by_code(code, domain_id, stats_days=90):
    """Look up product(s) by EAN/UPC/GTIN on a given marketplace."""
    data = keepa_get("product", {
        "domain": domain_id,
        "code": code,
        "stats": stats_days,
    })
    return data.get("products", [])

# ---------------------------------------------------------------------------
# PRICE HELPERS
# ---------------------------------------------------------------------------

def keepa_price_to_float(cents):
    if cents is None or cents < 0:
        return None
    return round(cents / 100, 2)

def get_current_buybox_price(product):
    stats = product.get("stats") or {}
    current = stats.get("current") or []
    # index 18 = buy box price in the "current" stats array
    if len(current) > 18:
        return keepa_price_to_float(current[18])
    # fallback: NEW price, index 1
    if len(current) > 1:
        return keepa_price_to_float(current[1])
    return None

def get_avg_price(product, field_index=18):
    stats = product.get("stats") or {}
    avg90 = stats.get("avg90") or []
    if len(avg90) > field_index:
        return keepa_price_to_float(avg90[field_index])
    return None

def get_primary_ean(product):
    eans = product.get("eanList") or []
    return eans[0] if eans else None

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

GRAPH_BASE = "https://api.keepa.com/graphimage"

def fetch_graph_image(asin, domain_id, days=90):
    """
    Fetch a Keepa price-history graph PNG for an ASIN.
    NOTE: never pass this URL directly into a Discord embed - it contains
    your API key. Always download the bytes here and upload as a Discord
    file attachment instead (see send_discord_alert).
    Cached by Keepa for 90 min per identical param set - re-requesting the
    same graph within that window doesn't cost extra tokens.
    """
    params = {
        "key": KEEPA_API_KEY,
        "domain": domain_id,
        "asin": asin,
        "salesrank": 1,
        "bb": 1,          # buy box line
        "range": days,
        "width": 720,
        "height": 320,
    }
    try:
        r = requests.get(GRAPH_BASE, params=params, timeout=20)
        r.raise_for_status()
        return r.content  # raw PNG bytes
    except Exception as e:
        print(f"  [!] Graph image fetch failed for {asin}: {e}")
        return None

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

def send_discord_alert(country, source_product, uk_product, calc, ean):
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

    # Fetch the UK price-history graph and attach it as a file — never link
    # the raw graphimage URL, since it carries your Keepa API key.
    files = None
    graph_bytes = fetch_graph_image(uk_asin, DOMAIN_UK)
    if graph_bytes:
        embed["image"] = {"url": "attachment://graph.png"}
        files = {"file": ("graph.png", graph_bytes, "image/png")}

    payload = {"embeds": [embed]}
    if files:
        requests.post(DISCORD_WEBHOOK_URL, data={"payload_json": json.dumps(payload)}, files=files, timeout=20)
    else:
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=15)

DOMAIN_TLD = {"France": "fr", "Germany": "de", "Italy": "it", "Spain": "es"}

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def process_country(country, domain_id, state):
    print(f"[{country}] fetching deals...")
    deals = fetch_deals(domain_id)
    print(f"[{country}] {len(deals)} deals returned")

    for deal in deals:
        asin = deal.get("asin")
        if not asin:
            continue

        source_product = fetch_product_by_asin(asin, domain_id)
        if not source_product:
            continue

        ean = get_primary_ean(source_product)
        if not ean:
            continue  # can't cross-match without a barcode

        state_key = f"{country}:{ean}"
        buy_price = get_current_buybox_price(source_product) or get_avg_price(source_product)
        if buy_price is None:
            continue

        # dedupe: skip if we've already alerted this EAN at this price band recently
        prev = state.get(state_key)
        if prev and abs(prev.get("buy_price", 0) - buy_price) < 0.5:
            continue

        uk_matches = fetch_products_by_code(ean, DOMAIN_UK)
        if not uk_matches:
            continue

        for uk_product in uk_matches:
            uk_sell_price = get_current_buybox_price(uk_product) or get_avg_price(uk_product)
            if uk_sell_price is None:
                continue

            calc = calc_profit(buy_price, uk_sell_price)

            if (calc["profit"] >= MIN_PROFIT_GBP
                    and calc["roi_pct"] >= MIN_ROI_PCT
                    and calc["margin_pct"] >= MIN_MARGIN_PCT):
                print(f"  MATCH: {source_product.get('title','')[:60]} "
                      f"profit=£{calc['profit']} roi={calc['roi_pct']}%")
                send_discord_alert(country, source_product, uk_product, calc, ean)
                state[state_key] = {
                    "buy_price": buy_price,
                    "uk_asin": uk_product.get("asin"),
                    "last_alerted": datetime.now(timezone.utc).isoformat(),
                    "profit": calc["profit"],
                    "roi_pct": calc["roi_pct"],
                }

        time.sleep(1)  # be gentle on token budget / rate limits


def main():
    state = load_state()
    for country, domain_id in SOURCE_DOMAINS.items():
        try:
            process_country(country, domain_id, state)
        except Exception as e:
            print(f"[{country}] ERROR: {e}")
        save_state(state)  # persist after each country in case a later one fails
        time.sleep(15)  # spread load across the run rather than bursting
    save_state(state)


if __name__ == "__main__":
    main()

