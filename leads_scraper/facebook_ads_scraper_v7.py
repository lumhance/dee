"""
LUMHANCE — Facebook Ad Library Scraper v7 (FINAL)
====================================================
Stufe 1: Handles sammeln (104 Keywords inkl. Funnel-CTAs)
Stufe 2: 3 parallele Workers, 25s Timeout
Ziel: 1000 Leads | 500 IG | 500 EM
"""

import asyncio
import csv
import json
import os
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
    # Coaching & Business (30)
    "high ticket coaching", "business coach", "online coach", "business mentor",
    "mastermind", "mentoring programm", "business skalieren", "umsatz steigern",
    "traumkunden gewinnen", "neukundengewinnung", "discovery call",
    "kostenloses webinar", "1zu1 coaching", "finanzielle freiheit",
    "als experte positionieren", "leads generieren", "marketing agentur",
    "unternehmensberater", "consulting", "coaching programm",
    "mindset coach", "performance coach", "erfolgscoach", "business aufbauen",
    "verkaufsstrategie", "gruppen coaching", "premium coaching",
    "10k im monat", "erste 100k", "6-stelliger umsatz",
    # Online Business (12)
    "digitalprodukt", "online kurs", "onlinekurs erstellen", "info produkt",
    "affiliate marketing", "dropshipping", "amazon fba", "e-commerce",
    "shopify store", "online shop", "membership seite", "funnel builder",
    # Fitness & Health (8)
    "fitness coach", "personal trainer", "abnehmen coaching", "ernährungsberatung",
    "gesundheitscoach", "yoga lehrer", "fitness studio", "online fitness",
    # Immo & Finance (7)
    "immobilien investment", "immobilienmakler", "trading coach", "krypto coach",
    "vermögensaufbau", "passives einkommen", "steuerberater",
    # Beauty & Lifestyle (4)
    "beauty coach", "hair stylist", "kosmetikstudio", "wellness",
    # Handwerk & Local (8)
    "handwerker", "dachdecker", "fensterbau", "gartenbau", "küchenstudio",
    "autohaus", "versicherungsmakler", "finanzberater",
    # Funnel-CTA Keywords (35) — trifft JEDEN der Ads schaltet
    "kostenloses webinar", "gratis webinar", "free masterclass",
    "kostenlose masterclass", "kostenloses training", "gratis training",
    "kostenloser workshop", "gratis workshop", "live webinar",
    "kostenloses strategiegespräch", "gratis erstgespräch", "strategiecall",
    "kennenlerngespräch", "beratungsgespräch", "erstberatung kostenlos",
    "potenzialanalyse", "analyse call",
    "kostenloses ebook", "gratis ebook", "kostenloser leitfaden",
    "gratis pdf", "checkliste download", "kostenlose vorlage", "freebie",
    "kostenlose challenge", "gratis challenge", "5 tage challenge",
    "bootcamp kostenlos", "intensiv workshop",
    "jetzt bewerben", "warteliste", "limitierte plätze",
    "dein nächster schritt", "erstgespräch buchen", "termin sichern",
]

COUNTRY = "DE"
TARGET_QUALITY_LEADS = 1000
MIN_INSTAGRAM = 500
MIN_EMAIL = 500
PARALLEL_WORKERS = 3
SCROLL_ROUNDS_PER_KEYWORD = 8
HEADLESS = True
OUTPUT_FOLDER = r"C:\Users\jonny\Desktop\LUMHANCE_Leads"
HANDLES_CACHE = os.path.join(OUTPUT_FOLDER, "handles_cache_v7.json")
VISITED_CACHE = os.path.join(OUTPUT_FOLDER, "visited_v7.json")

# Shared state
quality_leads = []
ig_total = 0
em_total = 0
visited_count = 0
visited_handles = set()
lock = asyncio.Lock()
stop_flag = False


def build_url(term, country):
    return (
        f"https://www.facebook.com/ads/library/"
        f"?active_status=active&ad_type=all&country={country}&q={quote(term)}"
        f"&sort_data[direction]=desc&sort_data[mode]=relevancy_monthly_grouped"
        f"&search_type=keyword_unordered&media_type=all"
    )


async def dismiss_popups(page):
    try:
        await page.evaluate("""
            () => {
                const btns = document.querySelectorAll(
                    'button[data-cookiebanner="accept_only_essential_button"],' +
                    'button[title*="Nur erforderliche"],' +
                    'button[title*="Only allow essential"],' +
                    'button[title*="Alle ablehnen"],' +
                    'button[title*="Decline"]'
                );
                btns.forEach(b => { try { b.click(); } catch(e) {} });
                document.querySelectorAll('div[aria-label="Schließen"], div[aria-label="Close"]')
                    .forEach(b => { if (b.getBoundingClientRect().top < 300) try { b.click(); } catch(e) {} });
                document.querySelectorAll('[data-cookiebanner], [data-testid*="cookie"]')
                    .forEach(b => { try { b.remove(); } catch(e) {} });
            }
        """)
    except: pass


async def extract_handles(page):
    return await page.evaluate("""
        () => {
            const found = new Map();
            const bl = new Set(['ads','business','help','policies','privacy','terms','l.php','login',
                'watch','reel','marketplace','pages','groups','events','gaming','news',
                'bookmarks','settings','notifications','messages','friends','profile.php',
                'people','public','photo','photos','video','videos','search','directory','home','me','share','sharer']);
            document.querySelectorAll('a[href*="facebook.com"]').forEach(link => {
                const m = link.href.match(/facebook\\.com\\/([^/?&#]+)/);
                if (!m) return;
                const h = m[1];
                if (bl.has(h.toLowerCase()) || h.length < 2 || h.length > 80) return;
                const n = (link.innerText || '').trim();
                if (!found.has(h)) found.set(h, n.substring(0, 100));
            });
            return Array.from(found.entries()).map(([h, n]) => ({handle: h, name: n}));
        }
    """)


async def scrape_keyword(context, term, country):
    page = None
    try:
        page = await context.new_page()
        await page.goto(build_url(term, country), wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(3)
        await dismiss_popups(page)
        try:
            await page.wait_for_selector('a[href*="facebook.com"]', timeout=10000)
        except:
            await page.close()
            return []
        for _ in range(SCROLL_ROUNDS_PER_KEYWORD):
            try:
                await page.evaluate("() => window.scrollBy(0, 1500)")
                await asyncio.sleep(1.5)
            except: break
        handles = await extract_handles(page)
        await page.close()
        return handles
    except:
        try:
            if page: await page.close()
        except: pass
        return []


EXTRACT_JS = """
    () => {
        const r = { page_title: '', instagram: '', website: '', email: '', phone: '', likes: '' };
        r.page_title = (document.title || '').replace(' | Facebook', '').trim();
        const links = document.querySelectorAll('a[href]');
        links.forEach(link => {
            let href = link.href || '';
            if (href.includes('l.facebook.com/l.php') || href.includes('/l.php')) {
                try { const u = new URL(href); const x = u.searchParams.get('u'); if (x) href = decodeURIComponent(x); } catch(e) {}
            }
            const igM = href.match(/instagram\\.com\\/([^/?&#]+)/i);
            if (igM && !r.instagram) {
                const h = igM[1];
                if (!['explore','accounts','reel','p','reels','stories'].includes(h.toLowerCase())) r.instagram = h;
            }
            if (href.startsWith('http') && !r.website &&
                !href.includes('facebook.com') && !href.includes('instagram.com') &&
                !href.includes('fb.com') && !href.includes('fbcdn.net') &&
                !href.includes('messenger.com') && !href.includes('wa.me') &&
                !href.includes('whatsapp.com') && !href.includes('tiktok.com') &&
                !href.includes('youtube.com') && !href.includes('linkedin.com') &&
                !href.includes('twitter.com') && !href.includes('x.com')) {
                const clean = href.split('?')[0].split('#')[0];
                if (clean.length < 250) r.website = clean;
            }
        });
        const bt = document.body.innerText || '';
        if (!r.instagram) {
            const m = bt.match(/instagram[^a-z0-9@]{0,5}@?([a-z0-9._]{3,30})/i);
            if (m) r.instagram = m[1];
        }
        const em = bt.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/g);
        if (em) { const x = em.find(e => !e.includes('facebook.com') && !e.includes('fb.com')); if (x) r.email = x; }
        const lm = bt.match(/([\\d.,]+)\\s*(Personen gefällt das|Follower|Abonnenten|people like this|followers)/i);
        if (lm) r.likes = lm[1];
        return r;
    }
"""


async def visit_page(context, handle):
    page = None
    try:
        page = await context.new_page()
        await page.goto(f"https://www.facebook.com/{handle}/about", wait_until="domcontentloaded", timeout=25000)
        await asyncio.sleep(2.5)
        await dismiss_popups(page)
        data = await page.evaluate(EXTRACT_JS)
        await page.close()
        return data
    except:
        try:
            if page: await page.close()
        except: pass
        return None


def save_csv(filename):
    leads = list(quality_leads)
    if not leads: return
    leads.sort(key=lambda a: (
        0 if (a.get('instagram') and a.get('email')) else 1,
        0 if a.get('instagram') else 2
    ))
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['Firmenname','Instagram Handle','Instagram URL','Website','Email','Telefon','Follower','Facebook Page','Keyword'])
        for l in leads:
            ig = l.get('instagram', '')
            w.writerow([
                l.get('page_title','') or l.get('found_name',''), ig,
                f'https://www.instagram.com/{ig}' if ig else '',
                l.get('website',''), l.get('email',''), l.get('phone',''),
                l.get('likes',''), f"https://www.facebook.com/{l.get('handle','')}",
                l.get('keyword','')
            ])


def save_visited():
    try:
        with open(VISITED_CACHE, 'w', encoding='utf-8') as f:
            json.dump(list(visited_handles), f)
    except: pass


async def worker(wid, context, queue, filename):
    global quality_leads, ig_total, em_total, visited_count, stop_flag
    while not stop_flag:
        try:
            handle, meta = queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        try:
            data = await asyncio.wait_for(visit_page(context, handle), timeout=30)
        except:
            data = None

        async with lock:
            visited_count += 1
            visited_handles.add(handle)

            if data:
                data['handle'] = handle
                data['keyword'] = meta.get('keyword', '')
                data['found_name'] = meta.get('name', '')
                ig = data.get('instagram', '')
                em = data.get('email', '')
                if ig or em:
                    quality_leads.append(data)
                    if ig: ig_total += 1
                    if em: em_total += 1
                    marker = "🎯" if (ig and em) else ("📸" if ig else "📧")
                    name = (data.get('page_title') or meta.get('name','') or handle)[:28]
                    info = f"@{ig}"[:26] if ig else em[:26]
                    pct = int(len(quality_leads)/TARGET_QUALITY_LEADS*100)
                    print(f"[{len(quality_leads):4}/{TARGET_QUALITY_LEADS}] IG:{ig_total:4} EM:{em_total:4} V:{visited_count:4} {marker} {name:28} {info} ({pct}%)", flush=True)

                    if len(quality_leads) % 25 == 0:
                        save_csv(filename)
                        save_visited()

            if (len(quality_leads) >= TARGET_QUALITY_LEADS and
                ig_total >= MIN_INSTAGRAM and em_total >= MIN_EMAIL):
                stop_flag = True


async def main():
    global stop_flag
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    filename = os.path.join(OUTPUT_FOLDER, f"LUMHANCE_1000_FINAL_{datetime.now().strftime('%Y-%m-%d_%H%M')}.csv")

    print("=" * 70)
    print(f"LUMHANCE — v7 FINAL ({PARALLEL_WORKERS} Workers, {len(SEARCH_TERMS)} Keywords)")
    print(f"Ziel: {TARGET_QUALITY_LEADS} Leads | Min {MIN_INSTAGRAM} IG | Min {MIN_EMAIL} EM")
    print("=" * 70, flush=True)

    # Load visited cache
    if os.path.exists(VISITED_CACHE):
        try:
            with open(VISITED_CACHE, 'r', encoding='utf-8') as f:
                visited_handles.update(json.load(f))
            print(f"[✓] {len(visited_handles)} bereits besuchte Handles übersprungen", flush=True)
        except: pass

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox', '--disable-dev-shm-usage']
        )

        # ─── STUFE 1: Handles sammeln ───
        all_handles = {}
        if os.path.exists(HANDLES_CACHE):
            try:
                with open(HANDLES_CACHE, 'r', encoding='utf-8') as f:
                    all_handles = json.load(f)
                print(f"[✓] {len(all_handles)} Handles aus Cache", flush=True)
            except: pass

        ctx1 = await browser.new_context(
            viewport={'width': 1400, 'height': 900},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='de-DE'
        )

        # SKIP STUFE 1 — nutze Cache direkt
        print(f"[✓] Stufe 1 übersprungen — nutze {len(all_handles)} Handles aus Cache", flush=True)

        try:
            await ctx1.close()
        except: pass

        print(f"\n[*] Gesamt Handles: {len(all_handles)} | Bereits besucht: {len(visited_handles)}", flush=True)

        # ─── STUFE 2: Parallel besuchen ───
        queue = asyncio.Queue()
        for handle, meta in all_handles.items():
            if handle not in visited_handles:
                queue.put_nowait((handle, meta))
        remaining = queue.qsize()
        print(f"[*] Zu besuchen: {remaining}", flush=True)
        print(f"\n### STUFE 2: PARALLEL VISITS ({PARALLEL_WORKERS} Workers) ###\n", flush=True)

        contexts = []
        for i in range(PARALLEL_WORKERS):
            ctx = await browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='de-DE'
            )
            contexts.append(ctx)

        workers = [worker(i, contexts[i], queue, filename) for i in range(PARALLEL_WORKERS)]
        await asyncio.gather(*workers, return_exceptions=True)

        try:
            await browser.close()
        except: pass

    save_csv(filename)
    save_visited()
    both = sum(1 for l in quality_leads if l.get('instagram') and l.get('email'))
    total = len(quality_leads)
    print(f"\n{'=' * 70}")
    print(f"FERTIG! {total} Quality Leads gespeichert")
    print(f"  → {ig_total} Instagram ({int(ig_total/total*100) if total else 0}%)")
    print(f"  → {em_total} Email ({int(em_total/total*100) if total else 0}%)")
    print(f"  → {both} mit BEIDEN")
    print(f"  → Besucht: {visited_count}")
    print(f"  → Datei: {filename}")
    print(f"{'=' * 70}")

    if total < TARGET_QUALITY_LEADS:
        print(f"\n⚠️  Nur {total}/{TARGET_QUALITY_LEADS} — mehr Keywords nötig für volle 1000!")
    if ig_total < MIN_INSTAGRAM:
        print(f"⚠️  IG {ig_total}/{MIN_INSTAGRAM} — Instagram-Ziel nicht erreicht")
    if em_total < MIN_EMAIL:
        print(f"⚠️  EM {em_total}/{MIN_EMAIL} — Email-Ziel nicht erreicht")


if __name__ == "__main__":
    asyncio.run(main())
