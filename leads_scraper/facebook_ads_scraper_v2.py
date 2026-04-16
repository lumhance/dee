"""
LUMHANCE — Facebook Ad Library Scraper v2
===========================================
Verbesserungen gegenüber v1:
- Crash-resistent: jedes Keyword bekommt eigene Browser-Page
- Bessere Keywords: Fokus auf 6-7 figure Unternehmer
- Klickt in Ads rein um mehr Daten zu extrahieren
- Versucht Instagram Handle + Website zu finden
- Zählt Anzahl aktiver Ads pro Firma (= Budget-Indikator)

─── SETUP ────────────────────────────────────────────────
Einmalig im Terminal:
   pip install playwright
   playwright install chromium

─── RUN ──────────────────────────────────────────────────
   python facebook_ads_scraper_v2.py
"""

import asyncio
import csv
import re
import sys
from datetime import datetime
from urllib.parse import quote

# Force UTF-8 on Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("[!] Playwright nicht installiert:")
    print("    pip install playwright")
    print("    playwright install chromium")
    exit(1)

# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════

# Keywords für 6-7 figure Unternehmer (die wirklich Budget haben)
SEARCH_TERMS = [
    "high ticket coaching",
    "mentoring programm",
    "online business aufbauen",
    "umsatz skalieren",
    "mastermind",
    "consulting",
    "b2b agentur",
    "marketing agentur",
    "e-commerce skalieren",
    "unternehmer mentor",
]

COUNTRY = "DE"
SCROLL_ROUNDS = 10
HEADLESS = False  # False = Browser sichtbar (empfohlen zum Testen)
SCROLL_DELAY = 2.5

# ═══════════════════════════════════════════════════════

def build_url(search_term, country):
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


async def scroll_page(page, rounds):
    print(f"  [*] Scrolle {rounds} Mal...")
    for i in range(rounds):
        try:
            await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(SCROLL_DELAY)
            if (i + 1) % 5 == 0:
                print(f"      → {i + 1}/{rounds}")
        except Exception as e:
            print(f"      [!] Scroll {i+1} failed: {str(e)[:100]}")
            break


async def extract_ads_from_page(page):
    """Extrahiert alle Ads mit Instagram/Website-Links falls vorhanden."""
    ads = await page.evaluate("""
        () => {
            const results = [];
            const seen = new Set();

            // Alle Ad-Container finden
            const allLinks = document.querySelectorAll('a[href*="facebook.com"]');

            allLinks.forEach(link => {
                const href = link.href;
                const pageMatch = href.match(/facebook\\.com\\/([^/?&#]+)/);
                if (!pageMatch) return;

                const pageHandle = pageMatch[1];
                const blacklist = ['ads', 'business', 'help', 'policies', 'privacy', 'terms', 'l.php', 'login', 'watch', 'reel', 'marketplace'];
                if (blacklist.includes(pageHandle)) return;
                if (seen.has(pageHandle)) return;
                seen.add(pageHandle);

                // Ad-Container finden (hoch im DOM-Tree)
                let container = link;
                for (let i = 0; i < 10; i++) {
                    if (container.parentElement) container = container.parentElement;
                    if (container.getAttribute && container.getAttribute('role') === 'article') break;
                }

                // Display Name
                let displayName = link.innerText.trim() || pageHandle;
                if (displayName.length > 100) displayName = displayName.substring(0, 100);

                // Ad Text sammeln
                let adText = '';
                let instagramHandle = '';
                let website = '';

                if (container) {
                    // Text extrahieren
                    const textEls = container.querySelectorAll('span[dir="auto"], div[style*="text-align"]');
                    const texts = Array.from(textEls)
                        .map(el => el.innerText.trim())
                        .filter(t => t.length > 15 && t.length < 1000);
                    adText = texts.slice(0, 3).join(' || ').substring(0, 800);

                    // Alle Links im Container checken für Instagram + Website
                    const containerLinks = container.querySelectorAll('a[href]');
                    containerLinks.forEach(cl => {
                        const h = cl.href || '';
                        // Instagram Handle
                        const igMatch = h.match(/instagram\\.com\\/([^/?&#]+)/i);
                        if (igMatch && !instagramHandle) {
                            instagramHandle = igMatch[1];
                        }
                        // Website (externe Links via l.php)
                        if (h.includes('l.php') && !website) {
                            try {
                                const u = new URL(h);
                                const real = u.searchParams.get('u');
                                if (real && !real.includes('facebook.com') && !real.includes('instagram.com')) {
                                    website = real.split('?')[0];
                                }
                            } catch(e) {}
                        }
                    });

                    // Instagram Handle auch im Text suchen
                    if (!instagramHandle) {
                        const igTextMatch = adText.match(/@([a-z0-9._]+)/i);
                        if (igTextMatch) instagramHandle = igTextMatch[1];
                    }
                }

                results.push({
                    page_name: displayName,
                    page_handle: pageHandle,
                    page_url: `https://www.facebook.com/${pageHandle}`,
                    instagram_handle: instagramHandle,
                    instagram_url: instagramHandle ? `https://www.instagram.com/${instagramHandle}` : '',
                    website: website,
                    ad_text: adText
                });
            });

            return results;
        }
    """)
    return ads


async def scrape_keyword(context, keyword, country):
    """Eigene Page pro Keyword — falls eine crasht, laufen andere weiter."""
    url = build_url(keyword, country)
    print(f"\n[*] Suche: '{keyword}' in {country}")

    page = None
    try:
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        await asyncio.sleep(5)

        # Cookie-Banner
        try:
            cookie_btn = await page.query_selector(
                'button[data-cookiebanner="accept_only_essential_button"], '
                'button[title*="Only allow essential"], '
                'button[title*="Nur erforderliche"], '
                'button[title*="Alle ablehnen"]'
            )
            if cookie_btn:
                await cookie_btn.click()
                await asyncio.sleep(2)
        except:
            pass

        # Warten bis mind. ein Link da ist
        try:
            await page.wait_for_selector('a[href*="facebook.com"]', timeout=20000)
        except:
            print(f"    [!] Timeout — keine Ads geladen")
            await page.close()
            return []

        await scroll_page(page, SCROLL_ROUNDS)
        ads = await extract_ads_from_page(page)

        for ad in ads:
            ad['search_term'] = keyword
            ad['country'] = country

        print(f"    → {len(ads)} Firmen (davon {sum(1 for a in ads if a['instagram_handle'])} mit Instagram)")

        try:
            await page.close()
        except:
            pass
        return ads

    except Exception as e:
        print(f"    [!] Fehler: {str(e)[:200]}")
        try:
            if page:
                await page.close()
        except:
            pass
        return []


def dedupe_and_merge(all_ads):
    """Deduplicate und merge Infos (wenn eine Firma in mehreren Keywords auftaucht)."""
    merged = {}
    for ad in all_ads:
        handle = ad.get('page_handle', '')
        if not handle:
            continue
        if handle not in merged:
            merged[handle] = ad.copy()
            merged[handle]['ad_count'] = 1
            merged[handle]['keywords'] = [ad.get('search_term', '')]
        else:
            # Merge: wenn Instagram/Website fehlt, aus neuem Eintrag übernehmen
            if not merged[handle]['instagram_handle'] and ad.get('instagram_handle'):
                merged[handle]['instagram_handle'] = ad['instagram_handle']
                merged[handle]['instagram_url'] = ad['instagram_url']
            if not merged[handle]['website'] and ad.get('website'):
                merged[handle]['website'] = ad['website']
            merged[handle]['ad_count'] += 1
            if ad.get('search_term') not in merged[handle]['keywords']:
                merged[handle]['keywords'].append(ad.get('search_term', ''))
    return list(merged.values())


def save_csv(ads, filename):
    if not ads:
        print("\n[!] Keine Ads zum Speichern.")
        return

    # Sortieren: Firmen mit Instagram zuerst, dann nach Ad-Count
    ads.sort(key=lambda a: (
        -1 if a.get('instagram_handle') else 1,
        -a.get('ad_count', 0)
    ))

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Firmenname',
            'Instagram Handle',
            'Instagram URL',
            'Website',
            'Facebook Page',
            'FB Handle',
            'Ad Count',
            'Gefunden bei',
            'Ad Text (Kurzauszug)',
        ])
        for ad in ads:
            writer.writerow([
                ad.get('page_name', ''),
                ad.get('instagram_handle', ''),
                ad.get('instagram_url', ''),
                ad.get('website', ''),
                ad.get('page_url', ''),
                ad.get('page_handle', ''),
                ad.get('ad_count', 1),
                ', '.join(ad.get('keywords', [ad.get('search_term', '')])),
                ad.get('ad_text', '')[:300],
            ])

    ig_count = sum(1 for a in ads if a.get('instagram_handle'))
    web_count = sum(1 for a in ads if a.get('website'))
    print(f"\n[OK] {len(ads)} Firmen in {filename} gespeichert")
    print(f"     → {ig_count} mit Instagram Handle")
    print(f"     → {web_count} mit Website")


async def main():
    print("=" * 60)
    print("LUMHANCE — Ad Library Scraper v2")
    print("=" * 60)
    print(f"Keywords: {len(SEARCH_TERMS)} Terms")
    print(f"Land: {COUNTRY}")
    print(f"Scroll-Rounds: {SCROLL_ROUNDS}")
    print("=" * 60)

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

        for term in SEARCH_TERMS:
            ads = await scrape_keyword(context, term, COUNTRY)
            all_ads.extend(ads)
            await asyncio.sleep(2)

        try:
            await browser.close()
        except:
            pass

    print(f"\n[*] Gesamt: {len(all_ads)} Ads (vor Dedup)")
    unique = dedupe_and_merge(all_ads)
    print(f"[*] Unique Firmen: {len(unique)}")

    filename = f"leads_v2_{datetime.now().strftime('%Y-%m-%d_%H%M')}.csv"
    save_csv(unique, filename)

    print("\n─── OUTREACH PRIORITÄT ───")
    print("1. Firmen mit Instagram Handle → direkt DM")
    print("2. Firmen mit Website → Email-Outreach")
    print("3. Firmen mit hohem Ad Count → haben Budget, priorisieren")


if __name__ == "__main__":
    asyncio.run(main())
