"""
نظام مراقبة نشاط البيع لمقتنياتك على OpenSea (Robinhood Chain).
مستقل تمامًا عن بوت الشراء — بوت تيليجرام خاص، ومحفظة/محافظ منفصلة للمراقبة فقط.

الفكرة:
  1. كل 15 دقيقة: يجيب كل NFTs المملوكة لمحافظك عبر Alchemy NFT API
  2. يراقب أحداث البيع (item_sold) على تلك المجموعات فقط عبر OpenSea Stream
  3. كل 30 دقيقة: يرسل رسالة واحدة مجمّعة لكل المجموعات "النشطة" (فيها بيع خلال آخر 30 دقيقة)
  4. مجموعة تخرج من القائمة النشطة تلقائيًا لو ما شافت بيع لمدة 30 دقيقة
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import requests
import websockets
from dotenv import load_dotenv

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
ALCHEMY_API_KEY = os.environ["ALCHEMY_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
WALLET_ADDRESSES = [
    addr.strip() for addr in os.environ["WALLET_ADDRESSES"].split(",") if addr.strip()
]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
COLLECTION_STATS_API = "https://api.opensea.io/api/v2/collections"
ALCHEMY_NFT_API_BASE = f"https://robinhood-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}"

TARGET_CHAIN = "robinhood"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
HOLDINGS_REFRESH_INTERVAL = 15 * 60      # كل 15 دقيقة
DIGEST_INTERVAL = 30 * 60                # كل 30 دقيقة
ACTIVITY_TIMEOUT = 30 * 60               # 30 دقيقة بدون بيع = تخرج من النشاط

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("resale-watcher")

# --- حالة النظام ---
# holdings: slug -> {"count": عدد القطع, "contract": العنوان, "name": الاسم}
holdings: dict[str, dict] = {}
# active_sales: slug -> آخر وقت بيع مكتشف (timestamp)
active_sales: dict[str, float] = {}

_floor_price_cache: dict[str, tuple[float, float]] = {}  # slug -> (price, ts)


# ---------------------------------------------------------------------------
# جلب مقتنياتك من كل المحافظ (Alchemy NFT API)
# ---------------------------------------------------------------------------

def fetch_holdings_for_wallet(wallet: str) -> list[dict]:
    """يرجع قائمة NFTs بكل صفحاتها لعنوان محفظة واحد."""
    all_nfts = []
    page_key = None
    try:
        while True:
            params = {"owner": wallet, "pageSize": 100, "withMetadata": "false"}
            if page_key:
                params["pageKey"] = page_key
            resp = requests.get(f"{ALCHEMY_NFT_API_BASE}/getNFTsForOwner", params=params, timeout=15)
            if resp.status_code != 200:
                log.warning(f"[Alchemy NFT] HTTP {resp.status_code} لمحفظة {wallet}")
                break
            data = resp.json()
            all_nfts.extend(data.get("ownedNfts", []))
            page_key = data.get("pageKey")
            if not page_key:
                break
    except Exception as e:
        log.error(f"[Alchemy NFT] خطأ لمحفظة {wallet}: {e}")
    return all_nfts


def slug_from_contract(contract_address: str) -> str | None:
    """يستخرج collection_slug من عنوان العقد عبر OpenSea (نحتاجه للربط مع Stream)."""
    try:
        resp = requests.get(
            f"https://api.opensea.io/api/v2/chain/{TARGET_CHAIN}/contract/{contract_address}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        return resp.json().get("collection")
    except Exception as e:
        log.warning(f"[OpenSea Contract] خطأ لـ {contract_address}: {e}")
        return None


async def refresh_holdings():
    """يحدّث قائمة المقتنيات الكاملة من كل المحافظ."""
    new_holdings: dict[str, dict] = {}

    for wallet in WALLET_ADDRESSES:
        nfts = await asyncio.to_thread(fetch_holdings_for_wallet, wallet)
        for nft in nfts:
            contract_address = (nft.get("contract") or {}).get("address")
            if not contract_address:
                continue

            # نتجنب استعلام OpenSea لكل قطعة — نجمع أولاً حسب العقد
            key = contract_address.lower()
            if key not in new_holdings:
                new_holdings[key] = {"count": 0, "contract": contract_address, "slug": None}
            new_holdings[key]["count"] += 1

    # الآن نحدد الـ slug لكل عقد فريد (استعلام واحد لكل مجموعة، مو لكل قطعة)
    result: dict[str, dict] = {}
    for key, entry in new_holdings.items():
        slug = await asyncio.to_thread(slug_from_contract, entry["contract"])
        if not slug:
            continue
        result[slug] = {
            "count": entry["count"],
            "contract": entry["contract"],
            "slug": slug,
        }

    holdings.clear()
    holdings.update(result)
    log.info(f"[مقتنيات] تحديث: {len(holdings)} مجموعة مختلفة، إجمالي القطع: {sum(h['count'] for h in holdings.values())}")


async def holdings_refresh_loop():
    while True:
        try:
            await refresh_holdings()
        except Exception as e:
            log.error(f"[مقتنيات] خطأ أثناء التحديث: {e}")
        await asyncio.sleep(HOLDINGS_REFRESH_INTERVAL)


# ---------------------------------------------------------------------------
# السعر الأرضي (floor price) — مع كاش بسيط لتفادي استعلامات زايدة
# ---------------------------------------------------------------------------

def fetch_floor_price(slug: str) -> float | None:
    now = time.time()
    cached = _floor_price_cache.get(slug)
    if cached and (now - cached[1] < 120):  # كاش دقيقتين
        return cached[0]
    try:
        resp = requests.get(
            f"{COLLECTION_STATS_API}/{slug}/stats",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code != 200:
            return cached[0] if cached else None
        data = resp.json()
        floor = (data.get("total") or {}).get("floor_price")
        if floor is None:
            return cached[0] if cached else None
        floor = float(floor)
        _floor_price_cache[slug] = (floor, now)
        return floor
    except Exception as e:
        log.warning(f"[Floor Price] خطأ لـ '{slug}': {e}")
        return cached[0] if cached else None


# ---------------------------------------------------------------------------
# تيليجرام
# ---------------------------------------------------------------------------

send_queue: "asyncio.Queue[str]" = asyncio.Queue()


def enqueue_message(text: str):
    send_queue.put_nowait(text)


async def telegram_sender():
    while True:
        text = await send_queue.get()
        try:
            await asyncio.to_thread(
                requests.post,
                f"{TELEGRAM_API}/sendMessage",
                data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=10,
            )
        except Exception as e:
            log.error(f"خطأ إرسال تليجرام: {e}")
        send_queue.task_done()
        await asyncio.sleep(1.05)


def build_digest_message() -> str:
    now = datetime.now(LOCAL_TZ).strftime("%H:%M")
    lines = [
        "🔥 <b>نشاط بيع على مقتنياتك</b>",
        f"🕐 {now}",
        "━━━━━━━━━━━━━━━━━━",
    ]
    for slug in active_sales:
        entry = holdings.get(slug)
        if not entry:
            continue
        floor = fetch_floor_price(slug)
        floor_line = f"{floor:.4f} ETH" if floor is not None else "غير متاح حاليًا"
        url = f"https://opensea.io/collection/{slug}"
        lines.append(
            f"\n💎 <b>{slug}</b>\n"
            f"📦 لديك: {entry['count']} قطعة\n"
            f"🏷️ السعر الأرضي الحالي: {floor_line}\n"
            f"🔗 {url}"
        )
    lines.append("\n━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


async def digest_loop():
    while True:
        await asyncio.sleep(DIGEST_INTERVAL)

        # نظف المجموعات اللي هدأ نشاطها
        now = time.time()
        for slug in list(active_sales.keys()):
            if now - active_sales[slug] > ACTIVITY_TIMEOUT:
                active_sales.pop(slug, None)

        if active_sales:
            enqueue_message(build_digest_message())
            log.info(f"[ملخص] أُرسلت رسالة مجمعة لـ {len(active_sales)} مجموعة نشطة.")


# ---------------------------------------------------------------------------
# الاتصال بـ OpenSea Stream — مراقبة أحداث البيع فقط على مجموعاتك
# ---------------------------------------------------------------------------

async def listen_opensea():
    msg_ref = 0
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info("متصل بـ OpenSea Stream — يراقب نشاط بيع مقتنياتك.")
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()

                while True:
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if isinstance(parsed, list) and len(parsed) == 5:
                        _jref, _ref, _topic, event_name, payload_wrapper = parsed
                    else:
                        continue

                    if event_name != "item_sold":
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    item = payload.get("item", {}) or {}
                    chain = (item.get("chain", {}) or {}).get("name", "")
                    if chain != TARGET_CHAIN:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug or slug not in holdings:
                        continue  # نراقب فقط مجموعات نملك منها قطع

                    was_active = slug in active_sales
                    active_sales[slug] = time.time()
                    if not was_active:
                        log.info(f"🔔 '{slug}': بدأ نشاط بيع جديد — أُضيف للرسالة المجمعة القادمة.")

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال خلال 3 ثوانٍ...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"خطأ غير متوقع: {e}. إعادة المحاولة خلال 5 ثوانٍ...")
            await asyncio.sleep(5)


async def run():
    enqueue_message(
        f"✅ نظام مراقبة نشاط البيع اشتغل — يراقب {len(WALLET_ADDRESSES)} محفظة.\n"
        f"جاري جلب المقتنيات الأولية..."
    )
    await refresh_holdings()  # تحميل أولي قبل ما نبدأ المراقبة
    await asyncio.gather(
        listen_opensea(),
        holdings_refresh_loop(),
        digest_loop(),
        telegram_sender(),
    )


def main():
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدويًا.")
            break
        except Exception as e:
            log.critical(f"توقف غير متوقع: {e}. إعادة التشغيل خلال {backoff} ثانية...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break


if __name__ == "__main__":
    main()
