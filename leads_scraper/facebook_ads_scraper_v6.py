"""
LUMHANCE — Facebook Ad Library Scraper v6 (PARALLEL MODE)
==========================================================
5 parallele Browser-Tabs → 5x schneller als v5.
Lädt Handles aus Cache, verarbeitet alle parallel.
"""

import asyncio
import csv
import json
import os
import sys
from datetime import datetime

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
TARGET_QUALITY_LEADS = 1000
MIN_INSTAGRAM = 500
MIN_EMAIL = 500
PARALLEL_WORKERS = 5
HEADLESS = True
OUTPUT_FOLDER = r"C:\Users\jonny\Desktop\LUMHANCE_Leads"
HANDLES_CACHE = os.path.join(OUTPUT_FOLDER, "handles_cache.json")

# Shared state
quality_leads = []
ig_total = 0
em_total = 0
visited_count = 0
lock = asyncio.Lock()
stop_flag = False


async def dismiss_popups(page):
    try:
        await page.evaluate("""
            () => {
                const cookieBtns = document.querySelectorAll(
                    'button[data-cookiebanner="accept_only_essential_button"],' +
                    'button[title*="Nur erforderliche"],' +
                    'button[title*="Only allow essential"],' +
                    'button[title*="Alle ablehnen"]'
                );
                cookieBtns.forEach(b => { try { b.click(); } catch(e) {} });
                const closeBtns = document.querySelectorAll(
                    'div[aria-label="Schließen"], div[aria-label="Close"]'
                );
                closeBtns.forEach(b => {
                    const r = b.getBoundingClientRect();
                    if (r.top < 300) { try { b.click(); } catch(e) {} }
                });
                const banners = document.querySelectorAll('[data-cookiebanner], [data-testid*="cookie"]');
                banners.forEach(b => { try { b.remove(); } catch(e) {} });
            }
        """)
    except:
        pass


async def visit_facebook_page(context, handle):
    page = None
    try:
        page = await context.new_page()
        url = f"https://www.facebook.com/{handle}/about"
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await asyncio.sleep(1.5)
        await dismiss_popups(page)

        data = await page.evaluate("""
            () => {
                const result = { page_title: '', instagram: '', website: '', email: '', phone: '', likes: '' };
                result.page_title = (document.title || '').replace(' | Facebook', '').trim();
                const links = document.querySelectorAll('a[href]');
                links.forEach(link => {
                    let href = link.href || '';
                    if (href.includes('l.facebook.com/l.php') || href.includes('/l.php')) {
                        try {
                            const u = new URL(href);
                            const real = u.searchParams.get('u');
                            if (real) href = decodeURIComponent(real);
                        } catch(e) {}
                    }
                    const igMatch = href.match(/instagram\\.com\\/([^/?&#]+)/i);
                    if (igMatch && !result.instagram) {
                        const h = igMatch[1];
                        if (h !== 'explore' && h !== 'accounts' && h !== 'reel' && h !== 'p') {
                            result.instagram = h;
                        }
                    }
                    if (href.startsWith('http') && !result.website &&
                        !href.includes('facebook.com') && !href.includes('instagram.com') &&
                        !href.includes('fb.com') && !href.includes('fbcdn.net') &&
                        !href.includes('messenger.com') && !href.includes('wa.me') &&
                        !href.includes('whatsapp.com') && !href.includes('tiktok.com') &&
                        !href.includes('youtube.com') && !href.includes('linkedin.com') &&
                        !href.includes('twitter.com') && !href.includes('x.com')) {
                        const clean = href.split('?')[0].split('#')[0];
                        if (clean.length < 250) result.website = clean;
                    }
                });
                const bodyText = document.body.innerText || '';
                if (!result.instagram) {
                    const m = bodyText.match(/instagram[^a-z0-9@]{0,5}@?([a-z0-9._]{3,30})/i);
                    if (m) result.instagram = m[1];
                }
                const emailMatch = bodyText.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/g);
                if (emailMatch) {
                    const real = emailMatch.find(e => !e.includes('facebook.com') && !e.includes('fb.com'));
                    if (real) result.email = real;
                }
                const likesMatch = bodyText.match(/([\\d.,]+)\\s*(Personen gefällt das|Follower|Abonnenten|people like this|followers)/i);
                if (likesMatch) result.likes = likesMatch[1];
                return result;
            }
        """)
        await page.close()
        return data
    except:
        try:
            if page: await page.close()
        except: pass
        return None


def save_csv(filename):
    leads_copy = list(quality_leads)
    if not leads_copy:
        return
    leads_copy.sort(key=lambda a: (0 if a.get('instagram') else 1, 0 if a.get('email') else 1))
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Firmenname', 'Instagram Handle', 'Instagram URL', 'Website', 'Email', 'Telefon', 'Follower', 'Facebook Page', 'Keyword'])
        for lead in leads_copy:
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


async def worker(worker_id, context, queue, filename):
    global quality_leads, ig_total, em_total, visited_count, stop_flag
    while not stop_flag:
        try:
            handle, meta = queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        try:
            data = await asyncio.wait_for(visit_facebook_page(context, handle), timeout=20)
        except:
            data = None

        async with lock:
            visited_count += 1
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
                    name = (data.get('page_title') or meta.get('name', '') or handle)[:28]
                    info = f"@{ig}"[:26] if ig else em[:26]
                    print(f"[W{worker_id}][{len(quality_leads):4}/{TARGET_QUALITY_LEADS}] IG:{ig_total:3} EM:{em_total:3} V:{visited_count:4} {marker} {name:28} {info}", flush=True)
                    if len(quality_leads) % 20 == 0:
                        save_csv(filename)

            if (len(quality_leads) >= TARGET_QUALITY_LEADS and
                ig_total >= MIN_INSTAGRAM and em_total >= MIN_EMAIL):
                stop_flag = True
                print(f"\n[✓] ZIEL ERREICHT: {len(quality_leads)} Leads | IG:{ig_total} | EM:{em_total}", flush=True)


async def main():
    global stop_flag
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    print("=" * 65)
    print(f"LUMHANCE — v6 PARALLEL ({PARALLEL_WORKERS} Workers)")
    print(f"Ziel: {TARGET_QUALITY_LEADS} Leads | Min {MIN_INSTAGRAM} IG | Min {MIN_EMAIL} EM")
    print("=" * 65, flush=True)

    if not os.path.exists(HANDLES_CACHE):
        print("[!] Kein Cache! Erst v5 laufen lassen.")
        return

    with open(HANDLES_CACHE, 'r', encoding='utf-8') as f:
        all_handles = json.load(f)
    print(f"[✓] {len(all_handles)} Handles aus Cache geladen", flush=True)

    filename = os.path.join(OUTPUT_FOLDER, f"LUMHANCE_Leads_1000_{datetime.now().strftime('%Y-%m-%d_%H%M')}.csv")

    queue = asyncio.Queue()
    for handle, meta in all_handles.items():
        queue.put_nowait((handle, meta))

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox', '--disable-dev-shm-usage']
        )
        contexts = []
        for i in range(PARALLEL_WORKERS):
            ctx = await browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='de-DE'
            )
            contexts.append(ctx)

        print(f"\n### PARALLEL VISITS ({PARALLEL_WORKERS} Workers) ###\n", flush=True)

        workers = [worker(i + 1, contexts[i], queue, filename) for i in range(PARALLEL_WORKERS)]
        await asyncio.gather(*workers, return_exceptions=True)

        try:
            await browser.close()
        except: pass

    save_csv(filename)
    both = sum(1 for l in quality_leads if l.get('instagram') and l.get('email'))
    print(f"\n{'=' * 65}")
    print(f"[OK] {len(quality_leads)} Quality Leads")
    print(f"     {filename}")
    print(f"{'=' * 65}")
    print(f"→ {ig_total} mit Instagram")
    print(f"→ {em_total} mit Email")
    print(f"→ {both} mit BEIDEN")
    print(f"→ Besucht: {visited_count}")


if __name__ == "__main__":
    asyncio.run(main())
