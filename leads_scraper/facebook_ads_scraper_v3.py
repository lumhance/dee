"""
LUMHANCE — Facebook Ad Library Scraper v3
===========================================
NEUE STRATEGIE: Klickt in jede Ad rein und extrahiert Instagram Handle
aus dem "Info zum Werbetreibenden" Tab — das ist der zuverlässigste Weg.

Workflow pro Keyword:
1. Öffne Ad Library mit Keyword
2. Scrolle 3-5x um Ads zu laden
3. Finde alle "Anzeige-Details anzeigen" Buttons
4. Für jeden:
   - Klick → Modal öffnet
   - Klick "Info zum Werbetreibenden"
   - Extrahiere Instagram Handle
   - Schließe Modal
5. CSV speichern

─── RUN ──────────────────────────────────────────────────
   python facebook_ads_scraper_v3.py
"""

import asyncio
import csv
import sys
from datetime import datetime
from urllib.parse import quote

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("[!] pip install playwright && playwright install chromium")
    exit(1)

# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════

SEARCH_TERMS = [
    "high ticket coaching",
    "mentoring programm",
    "online business",
    "mastermind",
    "marketing agentur",
]

COUNTRY = "DE"

# Wie oft initial scrollen bevor wir Ads durchklicken
INITIAL_SCROLL_ROUNDS = 4

# Max wie viele Ads pro Keyword durchklicken
MAX_ADS_PER_KEYWORD = 20

HEADLESS = False

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


async def dismiss_popups(page):
    """Cookie-Banner + Login-Popups wegmachen."""
    # Cookie Banner
    for selector in [
        'button[data-cookiebanner="accept_only_essential_button"]',
        'button[title*="Nur erforderliche"]',
        'button[title*="Only allow essential"]',
        'div[aria-label="Cookies erforderlich"] button',
    ]:
        try:
            btn = await page.query_selector(selector)
            if btn:
                await btn.click()
                await asyncio.sleep(1)
                return
        except:
            pass

    # Login-Popup (das "X" oben rechts)
    try:
        close_btn = await page.query_selector('div[aria-label="Schließen"], div[aria-label="Close"]')
        if close_btn:
            await close_btn.click()
            await asyncio.sleep(1)
    except:
        pass


async def scroll_a_bit(page, rounds):
    """Scrollt runter um ein paar Ads zu laden — aber nicht extrem."""
    for i in range(rounds):
        try:
            await page.evaluate("() => window.scrollBy(0, 1200)")
            await asyncio.sleep(2)
        except:
            break


async def find_detail_buttons(page):
    """Findet alle 'Anzeige-Details anzeigen' Buttons auf der Seite."""
    # Mehrere Selektor-Strategien weil Facebook die gerne ändert
    buttons = await page.evaluate("""
        () => {
            const btns = [];
            // Alle Buttons und Divs mit Text
            const all = document.querySelectorAll('div[role="button"], button, a');
            all.forEach((el, idx) => {
                const text = (el.innerText || '').trim().toLowerCase();
                if (
                    text.includes('anzeige-details') ||
                    text.includes('anzeigedetails') ||
                    text === 'see ad details' ||
                    text === 'anzeige details' ||
                    text.includes('see ad details') ||
                    text.includes('ad details')
                ) {
                    // Einmaligen Selector-Attribute setzen
                    el.setAttribute('data-lumhance-btn', 'ad-detail-' + idx);
                    btns.push('ad-detail-' + idx);
                }
            });
            return btns;
        }
    """)
    return buttons


async def extract_advertiser_info(page):
    """Aus dem offenen Modal: Klick auf 'Info zum Werbetreibenden' und extrahiere Daten."""
    info = {
        'page_name': '',
        'instagram_handle': '',
        'instagram_url': '',
        'fb_page_url': '',
        'page_creation': '',
        'page_likes': '',
        'raw_text': ''
    }

    try:
        # Warten bis Modal geladen ist
        await asyncio.sleep(2)

        # Klick auf "Info zum Werbetreibenden" Tab
        clicked = await page.evaluate("""
            () => {
                const all = document.querySelectorAll('div[role="tab"], div[role="button"], button, span');
                for (const el of all) {
                    const t = (el.innerText || '').trim().toLowerCase();
                    if (
                        t === 'info zum werbetreibenden' ||
                        t === 'about the advertiser' ||
                        t.includes('werbetreibenden') ||
                        t.includes('about the advertiser')
                    ) {
                        el.click();
                        return true;
                    }
                }
                return false;
            }
        """)

        if clicked:
            await asyncio.sleep(2)

        # Jetzt Daten extrahieren aus der Info-Sektion
        data = await page.evaluate("""
            () => {
                // Finde das offene Dialog/Modal
                const modal = document.querySelector('div[role="dialog"]');
                if (!modal) return null;

                const result = {
                    page_name: '',
                    instagram_handle: '',
                    instagram_url: '',
                    fb_page_url: '',
                    page_creation: '',
                    page_likes: '',
                    raw_text: ''
                };

                // Alle Links im Modal durchsuchen
                const links = modal.querySelectorAll('a[href]');
                links.forEach(link => {
                    const href = link.href || '';
                    // Instagram Link
                    const igMatch = href.match(/instagram\\.com\\/([^/?&#]+)/i);
                    if (igMatch && !result.instagram_handle) {
                        result.instagram_handle = igMatch[1];
                        result.instagram_url = 'https://www.instagram.com/' + igMatch[1];
                    }
                    // Facebook Page
                    const fbMatch = href.match(/facebook\\.com\\/([^/?&#]+)/i);
                    if (fbMatch && !result.fb_page_url) {
                        const h = fbMatch[1];
                        if (!['ads', 'business', 'help', 'l.php', 'login'].includes(h)) {
                            result.fb_page_url = 'https://www.facebook.com/' + h;
                        }
                    }
                });

                // Page Name: meist prominent oben im Modal
                const h2s = modal.querySelectorAll('h1, h2, span[dir="auto"]');
                for (const h of h2s) {
                    const t = (h.innerText || '').trim();
                    if (t && t.length > 2 && t.length < 100 && !result.page_name) {
                        result.page_name = t;
                        break;
                    }
                }

                // Alle Text im Modal als raw fallback
                const allText = modal.innerText || '';
                result.raw_text = allText.substring(0, 1500);

                // Instagram auch im Text suchen (falls nicht als Link gefunden)
                if (!result.instagram_handle) {
                    const igTextMatch = allText.match(/instagram[^a-z0-9@]*@?([a-z0-9._]{3,30})/i);
                    if (igTextMatch) {
                        result.instagram_handle = igTextMatch[1];
                        result.instagram_url = 'https://www.instagram.com/' + igTextMatch[1];
                    }
                }

                // Page Likes
                const likesMatch = allText.match(/(\\d[\\d.,]*)\\s*(Gefällt mir|likes|Abonnenten|followers)/i);
                if (likesMatch) result.page_likes = likesMatch[1];

                // Page Creation
                const createdMatch = allText.match(/(Seite erstellt|Page created)[^0-9]*(\\d{1,2}\\.?\\s*[a-zA-Zä]+\\s*\\d{4}|\\w+ \\d{1,2},\\s*\\d{4})/i);
                if (createdMatch) result.page_creation = createdMatch[2];

                return result;
            }
        """)

        if data:
            info.update(data)

    except Exception as e:
        print(f"      [!] Info-Extraktion Fehler: {str(e)[:150]}")

    return info


async def close_modal(page):
    """Schließt das offene Ad-Detail-Modal."""
    try:
        # ESC drücken
        await page.keyboard.press('Escape')
        await asyncio.sleep(1)
    except:
        pass

    try:
        # Oder auf X klicken
        await page.evaluate("""
            () => {
                const closeBtns = document.querySelectorAll('div[aria-label="Schließen"], div[aria-label="Close"]');
                for (const btn of closeBtns) {
                    const rect = btn.getBoundingClientRect();
                    if (rect.top < 200) {
                        btn.click();
                        return;
                    }
                }
            }
        """)
        await asyncio.sleep(1)
    except:
        pass


async def scrape_keyword(context, keyword, country):
    print(f"\n{'=' * 50}")
    print(f"[*] Keyword: '{keyword}'")
    print(f"{'=' * 50}")

    page = None
    results = []

    try:
        page = await context.new_page()
        await page.goto(build_url(keyword, country), wait_until="domcontentloaded", timeout=90000)
        await asyncio.sleep(5)
        await dismiss_popups(page)
        await asyncio.sleep(2)

        # Wait für erste Ads
        try:
            await page.wait_for_selector('a[href*="facebook.com"]', timeout=15000)
        except:
            print(f"    [!] Keine Ads für '{keyword}'")
            await page.close()
            return []

        # Ein bisschen scrollen um mehr Ads zu laden
        print(f"  [1] Scrolle initial...")
        await scroll_a_bit(page, INITIAL_SCROLL_ROUNDS)

        # Detail-Buttons finden
        print(f"  [2] Suche 'Anzeige-Details' Buttons...")
        button_ids = await find_detail_buttons(page)
        print(f"      → {len(button_ids)} Buttons gefunden")

        # Limitieren
        button_ids = button_ids[:MAX_ADS_PER_KEYWORD]

        if not button_ids:
            print(f"  [!] Keine Detail-Buttons gefunden für '{keyword}' — Facebook hat das Layout geändert?")
            await page.close()
            return []

        # Jeden Button durchklicken
        for idx, btn_id in enumerate(button_ids, 1):
            try:
                print(f"  [3.{idx}] Klicke Ad {idx}/{len(button_ids)}...")

                # Button per data-attribute finden und klicken
                clicked = await page.evaluate(f"""
                    () => {{
                        const el = document.querySelector('[data-lumhance-btn="{btn_id}"]');
                        if (el) {{
                            el.scrollIntoView({{block: 'center'}});
                            el.click();
                            return true;
                        }}
                        return false;
                    }}
                """)

                if not clicked:
                    continue

                await asyncio.sleep(2.5)

                # Info extrahieren
                info = await extract_advertiser_info(page)
                info['search_term'] = keyword
                info['country'] = country

                if info['page_name'] or info['instagram_handle']:
                    status = "📸" if info['instagram_handle'] else "  "
                    name = info.get('page_name', '???')[:40]
                    ig = info.get('instagram_handle', '')
                    print(f"      {status} {name} | IG: {ig}")
                    results.append(info)

                # Modal schließen
                await close_modal(page)
                await asyncio.sleep(1)

            except Exception as e:
                print(f"      [!] Fehler bei Ad {idx}: {str(e)[:100]}")
                try:
                    await close_modal(page)
                except:
                    pass

        try:
            await page.close()
        except:
            pass

        return results

    except Exception as e:
        print(f"  [!] Keyword Fehler: {str(e)[:200]}")
        try:
            if page:
                await page.close()
        except:
            pass
        return results


def dedupe(ads):
    seen = set()
    unique = []
    for ad in ads:
        key = ad.get('page_name', '') + '|' + ad.get('fb_page_url', '')
        if key and key not in seen:
            seen.add(key)
            unique.append(ad)
    return unique


def save_csv(ads, filename):
    if not ads:
        print("\n[!] Nichts zum Speichern.")
        return

    # IG-Leads zuerst
    ads.sort(key=lambda a: 0 if a.get('instagram_handle') else 1)

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Firmenname',
            'Instagram Handle',
            'Instagram URL',
            'Facebook Page',
            'Page Likes',
            'Page Created',
            'Gefunden bei',
            'Land'
        ])
        for ad in ads:
            writer.writerow([
                ad.get('page_name', ''),
                ad.get('instagram_handle', ''),
                ad.get('instagram_url', ''),
                ad.get('fb_page_url', ''),
                ad.get('page_likes', ''),
                ad.get('page_creation', ''),
                ad.get('search_term', ''),
                ad.get('country', ''),
            ])

    ig_count = sum(1 for a in ads if a.get('instagram_handle'))
    print(f"\n[OK] {len(ads)} Leads in {filename}")
    print(f"     → {ig_count} mit Instagram Handle ({int(ig_count/len(ads)*100) if ads else 0}%)")


async def main():
    print("=" * 60)
    print("LUMHANCE — Ad Library Scraper v3 (Deep Dive)")
    print("=" * 60)
    print(f"Keywords: {len(SEARCH_TERMS)}")
    print(f"Land: {COUNTRY}")
    print(f"Max Ads/Keyword: {MAX_ADS_PER_KEYWORD}")
    print("=" * 60)

    all_leads = []

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
            leads = await scrape_keyword(context, term, COUNTRY)
            all_leads.extend(leads)
            await asyncio.sleep(2)

        try:
            await browser.close()
        except:
            pass

    unique = dedupe(all_leads)
    filename = f"leads_v3_{datetime.now().strftime('%Y-%m-%d_%H%M')}.csv"
    save_csv(unique, filename)


if __name__ == "__main__":
    asyncio.run(main())
