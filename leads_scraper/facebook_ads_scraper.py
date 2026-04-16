"""
LUMHANCE — Facebook Ad Library Scraper
=======================================
Findet Firmen die gerade AKTIV Meta Ads schalten in DACH.
Output: CSV mit Firmenname, Page-Link, Ad-Text, Startdatum.

─── SETUP (einmalig) ─────────────────────────────────────
1. Geh auf https://developers.facebook.com/
2. Erstelle einen kostenlosen Developer Account
3. "My Apps" → "Create App" → Type: "Business" → weiter
4. In der App: "Settings" → "Basic" → kopier die App ID + App Secret
5. Geh auf https://developers.facebook.com/tools/explorer/
6. Wähle deine App aus → "Generate Access Token"
7. Kopier den Token und füg ihn unten bei ACCESS_TOKEN ein

─── RUN ──────────────────────────────────────────────────
   pip install requests
   python facebook_ads_scraper.py

Output: leads_YYYY-MM-DD.csv im selben Ordner
"""

import requests
import csv
import json
from datetime import datetime
import time
import sys

# ═══════════════════════════════════════════════════════
# CONFIG — HIER ANPASSEN
# ═══════════════════════════════════════════════════════

ACCESS_TOKEN = "HIER_DEINEN_TOKEN_EINFÜGEN"

# Nach welchen Keywords suchen? (eins pro Zeile)
SEARCH_TERMS = [
    "coaching",
    "online kurs",
    "fitness studio",
    "e-commerce",
    "personal trainer",
    # füg hier beliebig viele Keywords hinzu
]

# Welche Länder? DACH = DE, AT, CH
COUNTRIES = ["DE", "AT", "CH"]

# Wie viele Ads pro Keyword maximal ziehen? (API-Limit: 100 pro Request)
MAX_ADS_PER_KEYWORD = 200

# Nur aktive Ads oder alle?
ONLY_ACTIVE = True

# ═══════════════════════════════════════════════════════
# SCRAPER LOGIK — normalerweise nichts ändern
# ═══════════════════════════════════════════════════════

API_URL = "https://graph.facebook.com/v19.0/ads_archive"

FIELDS = [
    "id",
    "page_name",
    "page_id",
    "ad_creation_time",
    "ad_delivery_start_time",
    "ad_delivery_stop_time",
    "ad_creative_bodies",
    "ad_creative_link_titles",
    "ad_creative_link_descriptions",
    "ad_snapshot_url",
    "languages",
    "publisher_platforms",
]


def fetch_ads(search_term, country, max_results=200):
    """Holt Ads für einen Search Term in einem Land."""
    results = []
    params = {
        "access_token": ACCESS_TOKEN,
        "ad_reached_countries": f'["{country}"]',
        "search_terms": search_term,
        "ad_type": "ALL",
        "ad_active_status": "ACTIVE" if ONLY_ACTIVE else "ALL",
        "fields": ",".join(FIELDS),
        "limit": 100,
    }

    url = API_URL
    fetched = 0

    while url and fetched < max_results:
        try:
            if url == API_URL:
                resp = requests.get(url, params=params, timeout=30)
            else:
                resp = requests.get(url, timeout=30)

            if resp.status_code != 200:
                print(f"  [!] API Error {resp.status_code}: {resp.text[:200]}")
                break

            data = resp.json()

            if "error" in data:
                print(f"  [!] API Error: {data['error'].get('message', 'unknown')}")
                break

            batch = data.get("data", [])
            results.extend(batch)
            fetched += len(batch)

            # Pagination
            paging = data.get("paging", {})
            url = paging.get("next")

            if not batch:
                break

            time.sleep(0.5)  # Rate limit schonen

        except requests.exceptions.RequestException as e:
            print(f"  [!] Request failed: {e}")
            break

    return results[:max_results]


def dedupe_by_page(ads):
    """Entfernt Duplikate — nur ein Eintrag pro Firma."""
    seen = set()
    unique = []
    for ad in ads:
        page_id = ad.get("page_id")
        if page_id and page_id not in seen:
            seen.add(page_id)
            unique.append(ad)
    return unique


def extract_ad_text(ad):
    """Zieht den Ad-Copy Text raus."""
    bodies = ad.get("ad_creative_bodies") or []
    if bodies:
        return " | ".join(bodies)[:500]
    return ""


def save_to_csv(leads, filename):
    """Speichert Leads als CSV."""
    if not leads:
        print("[!] Keine Leads gefunden.")
        return

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Firmenname",
            "Page ID",
            "Facebook Page Link",
            "Ad Snapshot Link",
            "Ad Startdatum",
            "Ad Text (Kurzauszug)",
            "Sprachen",
            "Plattformen",
            "Search Term",
            "Land",
        ])

        for lead in leads:
            page_id = lead.get("page_id", "")
            writer.writerow([
                lead.get("page_name", ""),
                page_id,
                f"https://www.facebook.com/{page_id}" if page_id else "",
                lead.get("ad_snapshot_url", ""),
                lead.get("ad_delivery_start_time", ""),
                extract_ad_text(lead),
                ", ".join(lead.get("languages", []) or []),
                ", ".join(lead.get("publisher_platforms", []) or []),
                lead.get("_search_term", ""),
                lead.get("_country", ""),
            ])

    print(f"\n[OK] {len(leads)} Leads gespeichert in: {filename}")


def main():
    if ACCESS_TOKEN == "HIER_DEINEN_TOKEN_EINFÜGEN":
        print("[!] FEHLER: Du musst zuerst deinen Meta Access Token oben im Script einfügen.")
        print("    Siehe Anleitung am Anfang der Datei.")
        sys.exit(1)

    print("═" * 60)
    print("LUMHANCE — Facebook Ad Library Scraper")
    print("═" * 60)
    print(f"Suche nach {len(SEARCH_TERMS)} Keywords in {len(COUNTRIES)} Ländern")
    print(f"Max Ads pro Keyword: {MAX_ADS_PER_KEYWORD}")
    print(f"Filter: {'nur aktive Ads' if ONLY_ACTIVE else 'alle Ads'}")
    print("═" * 60)

    all_leads = []

    for term in SEARCH_TERMS:
        for country in COUNTRIES:
            print(f"\n[*] Suche: '{term}' in {country}...")
            ads = fetch_ads(term, country, MAX_ADS_PER_KEYWORD)

            # Metadaten anhängen
            for ad in ads:
                ad["_search_term"] = term
                ad["_country"] = country

            print(f"    → {len(ads)} Ads gefunden")
            all_leads.extend(ads)

    print(f"\n[*] Gesamt: {len(all_leads)} Ads vor Deduplizierung")
    unique_leads = dedupe_by_page(all_leads)
    print(f"[*] Nach Deduplizierung: {len(unique_leads)} unique Firmen")

    filename = f"leads_{datetime.now().strftime('%Y-%m-%d_%H%M')}.csv"
    save_to_csv(unique_leads, filename)

    print("\n─── NÄCHSTE SCHRITTE ───")
    print(f"1. Öffne {filename} in Excel / Google Sheets")
    print("2. Gehe durch die Firmen und check deren Ads (Ad Snapshot Link)")
    print("3. Die mit schwachen Creatives = perfekte Prospects für LUMHANCE")
    print("4. Outreach via Instagram DM / Email / Facebook")


if __name__ == "__main__":
    main()
