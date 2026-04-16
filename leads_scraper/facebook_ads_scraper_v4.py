"""
LUMHANCE — Facebook Ad Library Scraper v4
===========================================
NEUER ANSATZ: Zwei-Stufen-Scraping

STUFE 1: Ad Library durchsuchen → alle Facebook Page-Handles sammeln
STUFE 2: Jede gefundene Facebook Page besuchen → Instagram + Website
         + Page-Infos aus der About-Sektion extrahieren

Das ist zuverlässiger als Modal-Klicks weil Facebook Pages
eine stabile, gut strukturierte About-Sektion haben.

─── RUN ──────────────────────────────────────────────────
   python facebook_ads_scraper_v4.py
"""

import asyncio
import csv
import re
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

# Top Keywords für High-Ticket Coaches / Unternehmer mit Budget
# (gepickt aus der größeren Liste für maximale Prospect-Qualität)
SEARCH_TERMS = [
    # Geld-Keywords (direkt Indikatoren für High-Ticket)
    "high ticket coaching",
    "6 stelliges business",
    "7 stelliges business",
    "6-stelliger umsatz",
    "7-stelliger umsatz",
    "10k im monat",
    "20k im monat",
    "50k im monat",
    "erste 100k",
    "konstante 10k",

    # High-Ticket & Premium
    "high ticket angebot",
    "hochpreiscoaching",
    "premium coaching",
    "vip coaching",
    "elite coaching",

    # Coach-Typen
    "business coach",
    "online coach",
    "business mentor",
    "mastermind",
    "mentoring programm",

    # Skalieren / Wachstum
    "business skalieren",
    "umsatz steigern",
    "traumkunden gewinnen",
    "neukundengewinnung",

    # Funnel / Calls
    "discovery call",
    "strategy call",
    "kostenloses erstgespräch",
    "gratis masterclass",
    "kostenloses webinar",
    "1zu1 coaching",

    # Freiheit / Lifestyle
    "raus aus dem 9-5",
    "finanzielle freiheit",
    "ortsunabhängig arbeiten",
    "hamsterrad verlassen",

    # Marketing / Positionierung
    "persönliche marke aufbauen",
    "als experte positionieren",
    "sichtbar werden",
    "leads generieren",

    # Agentur / Consulting
    "marketing agentur",
    "unternehmensberater",
    "consulting",
]

COUNTRY = "DE"
MAX_PAGES_TO_VISIT = 800  # Wie viele FB Pages maximal besuchen
SCROLL_ROUNDS_PER_KEYWORD = 10  # mehr scrollen = mehr Pages pro Keyword
HEADLESS = True  # im Hintergrund laufen lassen (schneller, kein Fenster)

# ═══════════════════════════════════════════════════════


def build_ad_library_url(search_term, country):
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
    for selector in [
        'button[data-cookiebanner="accept_only_essential_button"]',
        'button[title*="Nur erforderliche"]',
        'button[title*="Only allow essential"]',
        'button[title*="Alle ablehnen"]',
    ]:
        try:
            btn = await page.query_selector(selector)
            if btn:
                await btn.click()
                await asyncio.sleep(1)
                return
        except:
            pass

    # Login-Popup
    try:
        close = await page.query_selector('div[aria-label="Schließen"], div[aria-label="Close"]')
        if close:
            await close.click()
            await asyncio.sleep(1)
    except:
        pass


async def scroll_page(page, rounds):
    for i in range(rounds):
        try:
            await page.evaluate("() => window.scrollBy(0, 1500)")
            await asyncio.sleep(2)
        except:
            break


async def extract_page_handles(page):
    """Findet alle Facebook Page Handles auf der Ad Library Suchseite."""
    handles = await page.evaluate("""
        () => {
            const found = new Map();
            const blacklist = new Set([
                'ads', 'business', 'help', 'policies', 'privacy', 'terms',
                'l.php', 'login', 'watch', 'reel', 'marketplace', 'pages',
                'groups', 'events', 'gaming', 'news', 'bookmarks', 'settings',
                'notifications', 'messages', 'friends', 'profile.php', 'people',
                'public', 'photo', 'photos', 'video', 'videos', 'search',
                'directory', 'home', 'me', 'share', 'sharer'
            ]);

            const links = document.querySelectorAll('a[href*="facebook.com"]');
            links.forEach(link => {
                const href = link.href;
                // Matche facebook.com/HANDLE (nur erste Pfad-Komponente)
                const match = href.match(/facebook\\.com\\/([^/?&#]+)/);
                if (!match) return;
                const handle = match[1];
                if (blacklist.has(handle.toLowerCase())) return;
                if (handle.length < 2 || handle.length > 80) return;

                // Display Name aus Link-Text
                const name = (link.innerText || '').trim();
                if (!found.has(handle)) {
                    found.set(handle, name.substring(0, 100));
                } else if (name && !found.get(handle)) {
                    found.set(handle, name.substring(0, 100));
                }
            });

            return Array.from(found.entries()).map(([h, n]) => ({handle: h, name: n}));
        }
    """)
    return handles


async def scrape_ad_library_for_handles(context, keyword, country):
    """STUFE 1: Sammelt Page-Handles aus Ad Library."""
    page = None
    try:
        page = await context.new_page()
        url = build_ad_library_url(keyword, country)
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(4)
        await dismiss_popups(page)

        try:
            await page.wait_for_selector('a[href*="facebook.com"]', timeout=15000)
        except:
            await page.close()
            return []

        await scroll_page(page, SCROLL_ROUNDS_PER_KEYWORD)
        handles = await extract_page_handles(page)

        await page.close()
        return handles
    except Exception as e:
        print(f"    [!] Ad Library Error: {str(e)[:150]}")
        try:
            if page:
                await page.close()
        except:
            pass
        return []


async def visit_facebook_page(context, handle):
    """STUFE 2: Besucht eine Facebook Page und extrahiert Instagram + Website."""
    page = None
    try:
        page = await context.new_page()
        url = f"https://www.facebook.com/{handle}/about"

        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)
        await dismiss_popups(page)

        # Extrahiere alle interessanten Daten
        data = await page.evaluate("""
            () => {
                const result = {
                    page_title: '',
                    instagram: '',
                    website: '',
                    email: '',
                    phone: '',
                    category: '',
                    likes: '',
                    all_external_links: []
                };

                // Page Title
                const title = document.title || '';
                result.page_title = title.replace(' | Facebook', '').trim();

                // Alle Links durchsuchen
                const links = document.querySelectorAll('a[href]');
                const seenLinks = new Set();
                links.forEach(link => {
                    let href = link.href || '';

                    // Facebook l.php → echte URL extrahieren
                    if (href.includes('l.facebook.com/l.php') || href.includes('/l.php')) {
                        try {
                            const u = new URL(href);
                            const real = u.searchParams.get('u');
                            if (real) href = decodeURIComponent(real);
                        } catch(e) {}
                    }

                    // Instagram
                    const igMatch = href.match(/instagram\\.com\\/([^/?&#]+)/i);
                    if (igMatch && !result.instagram) {
                        const h = igMatch[1];
                        if (h !== 'explore' && h !== 'accounts' && h !== 'reel') {
                            result.instagram = h;
                        }
                    }

                    // Externe Website (weder fb noch ig)
                    if (href.startsWith('http') &&
                        !href.includes('facebook.com') &&
                        !href.includes('instagram.com') &&
                        !href.includes('fb.com') &&
                        !href.includes('fbcdn.net') &&
                        !href.includes('messenger.com') &&
                        !href.includes('wa.me') &&
                        !href.includes('whatsapp.com') &&
                        !href.includes('tiktok.com') &&
                        !href.includes('youtube.com') &&
                        !href.includes('linkedin.com') &&
                        !href.includes('twitter.com') &&
                        !href.includes('x.com')) {
                        const cleanUrl = href.split('?')[0].split('#')[0];
                        if (!seenLinks.has(cleanUrl) && cleanUrl.length < 200) {
                            seenLinks.add(cleanUrl);
                            if (!result.website) result.website = cleanUrl;
                            result.all_external_links.push(cleanUrl);
                        }
                    }
                });

                // Fallback: Instagram aus Text
                if (!result.instagram) {
                    const bodyText = document.body.innerText || '';
                    const igTextMatch = bodyText.match(/instagram[^a-z0-9@]*@?([a-z0-9._]{3,30})/i);
                    if (igTextMatch) result.instagram = igTextMatch[1];
                }

                // Email aus Body
                const bodyText = document.body.innerText || '';
                const emailMatch = bodyText.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/);
                if (emailMatch) result.email = emailMatch[0];

                // Telefon — nur single-line, mit mind. einem Leerzeichen oder Bindestrich
                const phoneLines = bodyText.split('\\n').filter(l => l.length < 40);
                for (const line of phoneLines) {
                    const phoneMatch = line.match(/(?:\\+49|0049|0)\\s?[1-9][\\d]{1,4}[\\s\\-\\/][\\d\\s\\-\\/]{6,20}\\d/);
                    if (phoneMatch) {
                        result.phone = phoneMatch[0].trim().replace(/\\s+/g, ' ');
                        break;
                    }
                }

                // Likes / Follower
                const likesMatch = bodyText.match(/(\\d[\\d.,]*)\\s*(Personen gefällt das|Follower|Abonnenten|people like this|followers)/i);
                if (likesMatch) result.likes = likesMatch[1];

                return result;
            }
        """)

        await page.close()
        return data
    except Exception as e:
        try:
            if page:
                await page.close()
        except:
            pass
        return None


def save_csv(leads, filename):
    if not leads:
        print("\n[!] Nichts zum Speichern.")
        return

    # IG-Leads zuerst sortieren
    leads.sort(key=lambda a: (
        0 if a.get('instagram') else 1,
        0 if a.get('website') else 1
    ))

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Firmenname',
            'Instagram Handle',
            'Instagram URL',
            'Website',
            'Email',
            'Telefon',
            'Follower/Likes',
            'Facebook Page',
            'Gefunden bei Keyword',
        ])
        for lead in leads:
            ig = lead.get('instagram', '')
            writer.writerow([
                lead.get('page_title', '') or lead.get('found_name', ''),
                ig,
                f'https://www.instagram.com/{ig}' if ig else '',
                lead.get('website', ''),
                lead.get('email', ''),
                lead.get('phone', ''),
                lead.get('likes', ''),
                f"https://www.facebook.com/{lead.get('handle', '')}",
                lead.get('keyword', ''),
            ])

    ig = sum(1 for l in leads if l.get('instagram'))
    web = sum(1 for l in leads if l.get('website'))
    email = sum(1 for l in leads if l.get('email'))
    print(f"\n[OK] {len(leads)} Leads in {filename}")
    print(f"     → {ig} mit Instagram ({int(ig/len(leads)*100) if leads else 0}%)")
    print(f"     → {web} mit Website ({int(web/len(leads)*100) if leads else 0}%)")
    print(f"     → {email} mit Email ({int(email/len(leads)*100) if leads else 0}%)")


async def main():
    print("=" * 60)
    print("LUMHANCE — Ad Library Scraper v4 (Two-Stage)")
    print("=" * 60)
    print(f"Keywords: {len(SEARCH_TERMS)}")
    print(f"Max Pages: {MAX_PAGES_TO_VISIT}")
    print("=" * 60)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
        )
        context = await browser.new_context(
            viewport={'width': 1400, 'height': 900},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='de-DE'
        )

        # ─── STUFE 1: Handles sammeln ───
        print("\n### STUFE 1: Facebook Pages aus Ad Library sammeln ###\n")
        all_handles = {}  # handle -> {name, keyword}
        for i, term in enumerate(SEARCH_TERMS, 1):
            print(f"[{i}/{len(SEARCH_TERMS)}] '{term}'")
            handles = await scrape_ad_library_for_handles(context, term, COUNTRY)
            new_count = 0
            for h in handles:
                if h['handle'] not in all_handles:
                    all_handles[h['handle']] = {
                        'name': h['name'],
                        'keyword': term
                    }
                    new_count += 1
            print(f"    → {len(handles)} gefunden ({new_count} neu)")
            await asyncio.sleep(1)

        print(f"\n[*] Gesamt unique Facebook Pages: {len(all_handles)}")

        # Limitieren
        handles_list = list(all_handles.items())[:MAX_PAGES_TO_VISIT]
        print(f"[*] Besuche die ersten {len(handles_list)} Pages...\n")

        # ─── STUFE 2: Pages besuchen ───
        print("### STUFE 2: Jede Page besuchen, Instagram + Website extrahieren ###\n")
        leads = []
        for i, (handle, meta) in enumerate(handles_list, 1):
            print(f"[{i}/{len(handles_list)}] Besuche: {handle[:50]}")
            data = await visit_facebook_page(context, handle)
            if data:
                data['handle'] = handle
                data['keyword'] = meta['keyword']
                data['found_name'] = meta['name']
                leads.append(data)

                status = []
                if data.get('instagram'):
                    status.append(f"IG: @{data['instagram']}")
                if data.get('website'):
                    status.append(f"Web: {data['website'][:40]}")
                print(f"    → {' | '.join(status) if status else 'keine Extras'}")
            else:
                print(f"    → Fehler beim Besuch")

            # Kurze Pause zwischen Page-Besuchen (Rate Limit)
            await asyncio.sleep(0.8)

        try:
            await browser.close()
        except:
            pass

    filename = f"leads_v4_{datetime.now().strftime('%Y-%m-%d_%H%M')}.csv"
    save_csv(leads, filename)

    print("\n─── OUTREACH-PRIORITÄT ───")
    print("1. Leads mit Instagram → DM")
    print("2. Leads mit Email → direkte Email")
    print("3. Leads mit Website → Kontaktformular oder Email finden")


if __name__ == "__main__":
    asyncio.run(main())
