"""
LUMHANCE — Facebook Ad Library Browser Scraper
================================================
Scraped die Facebook Ad Library OHNE API-Key.
Nutzt Playwright (echter Browser) — simuliert einen menschlichen User.

─── SETUP (einmalig) ─────────────────────────────────────
Im Terminal im leads_scraper Ordner:

   pip install playwright
   playwright install chromium

Das installiert Playwright + einen Chromium Browser (ca. 300MB).

─── RUN ──────────────────────────────────────────────────
   python facebook_ads_browser_scraper.py

Output: leads_YYYY-MM-DD_HHMM.csv im selben Ordner

─── WICHTIG ──────────────────────────────────────────────
- Beim ersten Run öffnet sich ein Browser-Fenster
- HEADLESS = False ist default, damit du siehst was passiert
- Du musst einmalig bei Facebook einloggen (optional — geht oft auch ohne)
- Dann scrapet das Script automatisch alle Ads
"""

import asyncio
import csv
import sys
import time
from datetime import datetime
from urllib.parse import quote

# Force UTF-8 output on Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("[!] Playwright nicht installiert. Führe aus:")
    print("    pip install playwright")
    print("    playwright install chromium")
    exit(1)

# ═══════════════════════════════════════════════════════
# CONFIG — HIER ANPASSEN
# ═══════════════════════════════════════════════════════

# Keywords — nach welchen Nischen suchen?
SEARCH_TERMS = [
    "coaching",
    "online kurs",
    "fitness studio",
    "e-commerce",
    "personal trainer",
]

# Land: DE, AT, CH, US, etc.
COUNTRY = "DE"

# Wie weit runter scrollen? (mehr Scrolls = mehr Ads, aber länger)
SCROLL_ROUNDS = 15

# Browser sichtbar oder im Hintergrund?
# False = du siehst den Browser (empfohlen zum Testen)
# True = läuft unsichtbar im Hintergrund
HEADLESS = False

# Pause zwischen Scrolls (Sekunden) — nicht zu schnell sonst Block-Risiko
SCROLL_DELAY = 2

# ═══════════════════════════════════════════════════════
# SCRAPER LOGIK
# ═══════════════════════════════════════════════════════

def build_url(search_term, country):
    """Baut die Ad Library URL für einen Search Term."""
    encoded = quote(search_term)
    return (
        f"https://www.facebook.com/ads/library/"
        f"?active_status=active"
        f"&ad_type=all"
        f"&country={country}"
        f"&q={encoded}"
        f"&sort_data[direction]=desc"
        f"&sort_data[mode]=relevancy_monthly_grouped"
        f"&search_type=keyword_unordered"
        f"&media_type=all"
    )


async def scroll_and_collect(page, rounds):
    """Scrollt mehrmals runter um mehr Ads zu laden."""
    print(f"  [*] Scrolle {rounds} Mal um mehr Ads zu laden...")
    for i in range(rounds):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(SCROLL_DELAY)
        if (i + 1) % 5 == 0:
            print(f"      → Scroll {i + 1}/{rounds}")


async def extract_ads(page):
    """Zieht alle sichtbaren Ads aus der Seite."""
    ads = await page.evaluate("""
        () => {
            const results = [];
            // Jede Ad ist in einem Container mit bestimmten Klassen
            // Facebook rendert Ads in divs mit role="article" oder ähnlichen Strukturen
            const containers = document.querySelectorAll('div[role="article"], ._7jvw, ._99s5, .xh8yej3');

            // Fallback: finde alle Links zu Seiten
            const seen = new Set();
            const allLinks = document.querySelectorAll('a[href*="facebook.com"]');

            allLinks.forEach(link => {
                const href = link.href;
                // Facebook Page Links
                const pageMatch = href.match(/facebook\\.com\\/([^/?&#]+)/);
                if (pageMatch && !['ads', 'business', 'help', 'policies', 'privacy', 'terms'].includes(pageMatch[1])) {
                    const pageName = pageMatch[1];
                    if (seen.has(pageName)) return;
                    seen.add(pageName);

                    // Finde den umgebenden Ad-Container
                    let container = link.closest('div[role="article"]') || link.closest('.x1lliihq') || link.parentElement;

                    // Versuche Ad-Text zu extrahieren
                    let adText = '';
                    if (container) {
                        const textEls = container.querySelectorAll('div[style*="text-align"], span[dir="auto"]');
                        const texts = Array.from(textEls).map(el => el.innerText.trim()).filter(t => t.length > 20);
                        adText = texts.slice(0, 3).join(' | ').substring(0, 500);
                    }

                    // Page Display Name
                    let displayName = link.innerText.trim() || pageName;

                    results.push({
                        page_name: displayName,
                        page_handle: pageName,
                        page_url: `https://www.facebook.com/${pageName}`,
                        ad_text: adText
                    });
                }
            });

            return results;
        }
    """)
    return ads


async def scrape_keyword(context, keyword, country):
    """Scraped alle Ads für ein Keyword — mit eigener Page pro Keyword für Crash-Resistenz."""
    url = build_url(keyword, country)
    print(f"\n[*] Suche: '{keyword}' in {country}")

    page = None
    try:
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        await asyncio.sleep(6)  # Warten bis Ads geladen sind

        # Cookie-Banner schließen falls vorhanden
        try:
            cookie_btn = await page.query_selector('button[data-cookiebanner="accept_only_essential_button"], button[title*="Only allow essential"], button[title*="Nur erforderliche"]')
            if cookie_btn:
                await cookie_btn.click()
                await asyncio.sleep(2)
        except:
            pass

        # Warten bis mindestens ein Link da ist (Ads geladen)
        try:
            await page.wait_for_selector('a[href*="facebook.com"]', timeout=15000)
        except:
            print(f"    [!] Keine Ads geladen für '{keyword}' — skippe")
            await page.close()
            return []

        await scroll_and_collect(page, SCROLL_ROUNDS)
        ads = await extract_ads(page)

        print(f"    → {len(ads)} Ads gefunden")

        # Metadaten anhängen
        for ad in ads:
            ad['search_term'] = keyword
            ad['country'] = country

        await page.close()
        return ads

    except Exception as e:
        print(f"    [!] Fehler: {str(e)[:200]}")
        try:
            if page:
                await page.close()
        except:
            pass
        return []


def dedupe(all_ads):
    """Entfernt Duplikate."""
    seen = set()
    unique = []
    for ad in all_ads:
        key = ad.get('page_handle', '')
        if key and key not in seen:
            seen.add(key)
            unique.append(ad)
    return unique


def save_csv(ads, filename):
    """Speichert als CSV."""
    if not ads:
        print("\n[!] Keine Ads zum Speichern.")
        return

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Firmenname',
            'Handle',
            'Facebook Page',
            'Ad Text (Kurzauszug)',
            'Keyword',
            'Land'
        ])
        for ad in ads:
            writer.writerow([
                ad.get('page_name', ''),
                ad.get('page_handle', ''),
                ad.get('page_url', ''),
                ad.get('ad_text', ''),
                ad.get('search_term', ''),
                ad.get('country', '')
            ])
    print(f"\n[OK] {len(ads)} unique Firmen in {filename} gespeichert")


async def main():
    print("═" * 60)
    print("LUMHANCE — Facebook Ad Library Browser Scraper")
    print("═" * 60)
    print(f"Keywords: {', '.join(SEARCH_TERMS)}")
    print(f"Land: {COUNTRY}")
    print(f"Scroll-Rounds: {SCROLL_ROUNDS}")
    print(f"Headless: {HEADLESS}")
    print("═" * 60)

    all_ads = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
            ]
        )
        context = await browser.new_context(
            viewport={'width': 1400, 'height': 900},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='de-DE'
        )
        page = await context.new_page()

        for term in SEARCH_TERMS:
            ads = await scrape_keyword(page, term, COUNTRY)
            all_ads.extend(ads)
            # Kurze Pause zwischen Keywords
            await asyncio.sleep(3)

        await browser.close()

    print(f"\n[*] Gesamt: {len(all_ads)} Ads (vor Dedup)")
    unique_ads = dedupe(all_ads)
    print(f"[*] Unique Firmen: {len(unique_ads)}")

    filename = f"leads_{datetime.now().strftime('%Y-%m-%d_%H%M')}.csv"
    save_csv(unique_ads, filename)

    print("\n─── NÄCHSTE SCHRITTE ───")
    print(f"1. Öffne {filename} in Excel / Google Sheets")
    print("2. Klick auf die Facebook Page Links → Ads anschauen")
    print("3. Firmen mit schwachen Creatives = perfekte Prospects")
    print("4. DM / Email Outreach")


if __name__ == "__main__":
    asyncio.run(main())
