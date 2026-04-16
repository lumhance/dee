"""
LUMHANCE — Facebook Ad Library Scraper v5 (QUALITY MODE)
==========================================================
Ziel: 100 Leads mit Instagram ODER Email — crash-resistent mit Disk-Cache.
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
    # Original coaching/business
    "high ticket coaching", "business coach", "online coach", "business mentor",
    "mastermind", "mentoring programm", "business skalieren", "umsatz steigern",
    "traumkunden gewinnen", "neukundengewinnung", "discovery call",
    "kostenloses webinar", "1zu1 coaching", "finanzielle freiheit",
    "als experte positionieren", "leads generieren", "marketing agentur",
    "unternehmensberater", "consulting", "coaching programm",
    "mindset coach", "performance coach", "erfolgscoach", "business aufbauen",
    "verkaufsstrategie", "gruppen coaching", "premium coaching",
    "10k im monat", "erste 100k", "6-stelliger umsatz",
    # Erweitert: Online Business
    "digitalprodukt", "online kurs", "onlinekurs erstellen", "info produkt",
    "affiliate marketing", "dropshipping", "amazon fba", "e-commerce",
    "shopify store", "online shop", "membership seite", "funnel builder",
    # Erweitert: Fitness/Health
    "fitness coach", "personal trainer", "abnehmen coaching", "ernährungsberatung",
    "gesundheitscoach", "yoga lehrer", "fitness studio", "online fitness",
    # Erweitert: Immobilien/Finanzen
    "immobilien investment", "immobilienmakler", "trading coach", "krypto coach",
    "vermögensaufbau", "passives einkommen", "steuerberater",
    # Erweitert: Beauty/Lifestyle
    "beauty coach", "hair stylist", "kosmetikstudio", "wellness",
    # Erweitert: Handwerk/Local
    "handwerker", "dachdecker", "fensterbau", "gartenbau", "küchenstudio",
    "autohaus", "versicherungsmakler", "finanzberater"
]

COUNTRY = "DE"
TARGET_QUALITY_LEADS = 1000
MIN_INSTAGRAM = 500
MIN_EMAIL = 500
MAX_TOTAL_VISITS = 3000
MIN_HANDLES_BEFORE_STAGE2 = 9999  # nie triggern — alle Keywords scrapen
SCROLL_ROUNDS_PER_KEYWORD = 10
HEADLESS = True
OUTPUT_FOLDER = r"C:\Users\jonny\Desktop\LUMHANCE_Leads"

# ═══════════════════════════════════════════════════════


def build_ad_library_url(search_term, country):
    encoded = quote(search_term)
    return (
        f"https://www.facebook.com/ads/library/"
        f"?active_status=active&ad_type=all&country={country}&q={encoded}"
        f"&sort_data[direction]=desc&sort_data[mode]=relevancy_monthly_grouped"
        f"&search_type=keyword_unordered&media_type=all"
    )


async def dismiss_popups(page):
    """Nutzt JavaScript-Click um Cookie-Intercept zu umgehen — kein Hängen mehr."""
    try:
        await page.evaluate("""
            () => {
                // Cookie-Banner via JS-Click
                const cookieBtns = document.querySelectorAll(
                    'button[data-cookiebanner="accept_only_essential_button"],' +
                    'button[title*="Nur erforderliche"],' +
                    'button[title*="Only allow essential"],' +
                    'button[title*="Alle ablehnen"]'
                );
                cookieBtns.forEach(b => { try { b.click(); } catch(e) {} });

                // Login-Popup schließen
                const closeBtns = document.querySelectorAll(
                    'div[aria-label="Schließen"],' +
                    'div[aria-label="Close"]'
                );
                closeBtns.forEach(b => {
                    const r = b.getBoundingClientRect();
                    if (r.top < 300) { try { b.click(); } catch(e) {} }
                });

                // Cookie-Banner DOM-Element komplett entfernen (falls Click nicht greift)
                const banners = document.querySelectorAll('[data-cookiebanner], [data-testid*="cookie"]');
                banners.forEach(b => { try { b.remove(); } catch(e) {} });
            }
        """)
    except:
        pass


async def scroll_page(page, rounds):
    for i in range(rounds):
        try:
            await page.evaluate("() => window.scrollBy(0, 1500)")
            await asyncio.sleep(1.5)
        except:
            break


async def extract_page_handles(page):
    handles = await page.evaluate("""
        () => {
            const found = new Map();
            const igBlacklist = new Set([
                'explore','accounts','reel','p','stories','reels','about',
                'developer','legal','privacy','terms_and_conditions'
            ]);
            const fbBlacklist = new Set([
                'ads','business','help','policies','privacy','terms','l.php','login',
                'watch','reel','marketplace','pages','groups','events','gaming','news',
                'bookmarks','settings','notifications','messages','friends','profile.php',
                'people','public','photo','photos','video','videos','search','directory',
                'home','me','share','sharer'
            ]);

            // Alle Instagram Handles aus der Ads Library sammeln
            const igMap = new Map();
            const igLinks = document.querySelectorAll('a[href*="instagram.com"]');
            igLinks.forEach(link => {
                const href = link.href || '';
                const m = href.match(/instagram\\.com\\/([a-zA-Z0-9._]{2,30})/i);
                if (!m) return;
                const igHandle = m[1].toLowerCase();
                if (igBlacklist.has(igHandle)) return;
                // Versuche den zugehörigen Facebook-Link im selben Ad-Container zu finden
                const container = link.closest('[class]') || link.parentElement;
                if (container) {
                    const fbLink = container.querySelector('a[href*="facebook.com"]');
                    if (fbLink) {
                        const fbMatch = fbLink.href.match(/facebook\\.com\\/([^/?&#]+)/);
                        if (fbMatch && !fbBlacklist.has(fbMatch[1].toLowerCase())) {
                            igMap.set(fbMatch[1], igHandle);
                        }
                    }
                }
            });

            // Alle Facebook Handles sammeln
            const links = document.querySelectorAll('a[href*="facebook.com"]');
            links.forEach(link => {
                const href = link.href;
                const match = href.match(/facebook\\.com\\/([^/?&#]+)/);
                if (!match) return;
                const handle = match[1];
                if (fbBlacklist.has(handle.toLowerCase())) return;
                if (handle.length < 2 || handle.length > 80) return;
                const name = (link.innerText || '').trim();
                const ig = igMap.get(handle) || '';
                if (!found.has(handle)) found.set(handle, {name: name.substring(0, 100), ig: ig});
                else {
                    const existing = found.get(handle);
                    if (name && !existing.name) existing.name = name.substring(0, 100);
                    if (ig && !existing.ig) existing.ig = ig;
                }
            });

            // Auch Instagram Handles ohne zugehörigen FB-Link im selben Container speichern
            // (fallback: ordne sie keinem FB-Handle zu, aber sammle sie separat)
            const orphanIGs = [];
            igLinks.forEach(link => {
                const href = link.href || '';
                const m = href.match(/instagram\\.com\\/([a-zA-Z0-9._]{2,30})/i);
                if (!m) return;
                const igHandle = m[1].toLowerCase();
                if (igBlacklist.has(igHandle)) return;
                // Check ob dieser IG-Handle bereits einem FB-Handle zugeordnet ist
                let alreadyMapped = false;
                for (const [, data] of found) {
                    if (data.ig === igHandle) { alreadyMapped = true; break; }
                }
                if (!alreadyMapped) {
                    const name = (link.innerText || '').trim();
                    orphanIGs.push({handle: igHandle, name: name.substring(0, 100)});
                }
            });

            const result = Array.from(found.entries()).map(([h, d]) => ({
                handle: h, name: d.name, ig: d.ig
            }));
            return {fbHandles: result, orphanIGs: orphanIGs};
        }
    """)
    return handles


async def scrape_handles_for_keyword(context, keyword, country):
    page = None
    try:
        page = await context.new_page()
        await page.goto(build_ad_library_url(keyword, country), wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(3)
        await dismiss_popups(page)
        try:
            await page.wait_for_selector('a[href*="facebook.com"]', timeout=10000)
        except:
            await page.close()
            return []
        await scroll_page(page, SCROLL_ROUNDS_PER_KEYWORD)
        handles = await extract_page_handles(page)
        await page.close()
        return handles
    except Exception as e:
        try:
            if page: await page.close()
        except: pass
        return []


async def visit_facebook_page(context, handle):
    page = None
    try:
        page = await context.new_page()
        url = f"https://www.facebook.com/{handle}/about"
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)
        await dismiss_popups(page)

        data = await page.evaluate("""
            () => {
                const result = {
                    page_title: '', instagram: '', website: '', email: '',
                    phone: '', likes: ''
                };
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

                const lines = bodyText.split('\\n').filter(l => l.length > 5 && l.length < 50);
                for (const line of lines) {
                    const m = line.match(/(?:\\+49|0049|0)\\s?[1-9][\\d]{1,4}[\\s\\-\\/][\\d\\s\\-\\/]{5,20}\\d/);
                    if (m) {
                        result.phone = m[0].trim().replace(/\\s+/g, ' ').substring(0, 30);
                        break;
                    }
                }

                const likesMatch = bodyText.match(/([\\d.,]+)\\s*(Personen gefällt das|Follower|Abonnenten|people like this|followers)/i);
                if (likesMatch) result.likes = likesMatch[1];

                return result;
            }
        """)

        await page.close()
        return data
    except Exception as e:
        try:
            if page: await page.close()
        except: pass
        return None


def save_csv(leads, filename):
    if not leads:
        return
    leads.sort(key=lambda a: (0 if a.get('instagram') else 1, 0 if a.get('email') else 1))
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Firmenname', 'Instagram Handle', 'Instagram URL', 'Website',
            'Email', 'Telefon', 'Follower', 'Facebook Page', 'Keyword'
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


async def main():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    print("=" * 60)
    print("LUMHANCE — Ad Library Scraper v5 (QUALITY MODE)")
    print("=" * 60)
    print(f"Keywords: {len(SEARCH_TERMS)}")
    print(f"Ziel: {TARGET_QUALITY_LEADS} Leads mit Instagram ODER Email")
    print("=" * 60, flush=True)

    filename = os.path.join(OUTPUT_FOLDER, f"LUMHANCE_Leads_Run2_{datetime.now().strftime('%Y-%m-%d_%H%M')}.csv")
    handles_cache_file = os.path.join(OUTPUT_FOLDER, "handles_cache.json")
    quality_leads = []

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

        # ─── STUFE 1 ───
        all_handles = {}
        if os.path.exists(handles_cache_file):
            try:
                with open(handles_cache_file, 'r', encoding='utf-8') as f:
                    all_handles = json.load(f)
                print(f"\n[✓] {len(all_handles)} Handles aus Cache geladen")
            except:
                all_handles = {}

        if len(all_handles) < MIN_HANDLES_BEFORE_STAGE2:
            print("\n### STUFE 1: Facebook Pages sammeln ###\n", flush=True)
            for i, term in enumerate(SEARCH_TERMS, 1):
                try:
                    print(f"[{i}/{len(SEARCH_TERMS)}] '{term}'", end=' ', flush=True)
                    result = await asyncio.wait_for(
                        scrape_handles_for_keyword(context, term, COUNTRY),
                        timeout=60
                    )
                    # result ist jetzt {fbHandles: [...], orphanIGs: [...]}
                    handles = result.get('fbHandles', []) if isinstance(result, dict) else result
                    new_count = 0
                    for h in handles:
                        hid = h['handle'] if isinstance(h, dict) else h
                        name = h.get('name', '') if isinstance(h, dict) else ''
                        ig = h.get('ig', '') if isinstance(h, dict) else ''
                        if hid not in all_handles:
                            all_handles[hid] = {'name': name, 'keyword': term, 'ig_from_ads': ig}
                            new_count += 1
                        elif ig and not all_handles[hid].get('ig_from_ads'):
                            all_handles[hid]['ig_from_ads'] = ig
                    print(f"→ +{new_count} (total: {len(all_handles)})", flush=True)

                    # Cache sofort speichern
                    try:
                        with open(handles_cache_file, 'w', encoding='utf-8') as f:
                            json.dump(all_handles, f, ensure_ascii=False)
                    except:
                        pass

                except asyncio.TimeoutError:
                    print(f"→ TIMEOUT", flush=True)
                except Exception as e:
                    print(f"→ ERROR: {str(e)[:60]}", flush=True)

                await asyncio.sleep(0.3)

                if len(all_handles) >= MIN_HANDLES_BEFORE_STAGE2:
                    print(f"\n[✓] {len(all_handles)} Handles gesammelt — springe zu Stufe 2", flush=True)
                    break

        print(f"\n[*] Gesamt unique Pages: {len(all_handles)}\n", flush=True)

        # ─── STUFE 2 ───
        print(f"### STUFE 2: Besuche Pages bis {TARGET_QUALITY_LEADS} Quality Leads ###\n", flush=True)

        handles_list = list(all_handles.items())
        visited_count = 0

        ig_total = 0
        em_total = 0

        for handle, meta in handles_list:
            quota_met = (
                len(quality_leads) >= TARGET_QUALITY_LEADS and
                ig_total >= MIN_INSTAGRAM and
                em_total >= MIN_EMAIL
            )
            if quota_met:
                print(f"\n[✓] Quota erreicht: {len(quality_leads)} Leads | {ig_total} IG | {em_total} Email!", flush=True)
                break
            if visited_count >= MAX_TOTAL_VISITS:
                print(f"\n[!] Max-Limit ({MAX_TOTAL_VISITS}) erreicht", flush=True)
                break

            visited_count += 1

            try:
                data = await asyncio.wait_for(visit_facebook_page(context, handle), timeout=25)
            except:
                data = None

            if data:
                data['handle'] = handle
                data['keyword'] = meta['keyword']
                data['found_name'] = meta['name']

                # Instagram aus Ads Library hat Priorität (direkt vom Werbetreibenden)
                ig_from_ads = meta.get('ig_from_ads', '')
                ig_from_page = data.get('instagram', '')
                ig = ig_from_ads or ig_from_page
                data['instagram'] = ig

                em = data.get('email', '')
                has_quality = bool(ig or em)

                if has_quality:
                    quality_leads.append(data)
                    if ig: ig_total += 1
                    if em: em_total += 1

                    marker = "🎯" if (ig and em) else ("📸" if ig else "📧")
                    name = (data.get('page_title') or meta['name'] or handle)[:30]
                    info = f"@{ig}"[:28] if ig else em[:28]
                    print(f"[{len(quality_leads):4}/{TARGET_QUALITY_LEADS}] IG:{ig_total:3} EM:{em_total:3} {marker} {name:30} {info}", flush=True)

                    if len(quality_leads) % 10 == 0:
                        save_csv(quality_leads, filename)

            # Browser-Restart alle 300 Visits um Memory-Leaks zu vermeiden
            if visited_count > 0 and visited_count % 300 == 0:
                try:
                    print(f"\n[*] Browser-Refresh nach {visited_count} Visits...", flush=True)
                    await context.close()
                    context = await browser.new_context(
                        viewport={'width': 1400, 'height': 900},
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        locale='de-DE'
                    )
                except: pass

            await asyncio.sleep(0.5)

        try:
            await browser.close()
        except:
            pass

    save_csv(quality_leads, filename)

    ig_count = sum(1 for l in quality_leads if l.get('instagram'))
    em_count = sum(1 for l in quality_leads if l.get('email'))
    both = sum(1 for l in quality_leads if l.get('instagram') and l.get('email'))

    print(f"\n{'=' * 60}")
    print(f"[OK] {len(quality_leads)} Quality Leads")
    print(f"     {filename}")
    print(f"{'=' * 60}")
    print(f"→ {ig_count} mit Instagram ({int(ig_count/len(quality_leads)*100) if quality_leads else 0}%)")
    print(f"→ {em_count} mit Email ({int(em_count/len(quality_leads)*100) if quality_leads else 0}%)")
    print(f"→ {both} mit BEIDEN")
    print(f"→ Besucht: {visited_count}")


if __name__ == "__main__":
    asyncio.run(main())
