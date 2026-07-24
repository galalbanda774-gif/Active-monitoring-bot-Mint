"""
نظام مراقبة نشاط البيع لمقتنياتك على OpenSea — يدعم Robinhood Chain + Ethereum Mainnet.
مستقل تمامًا عن بوت الشراء — بوت تيليجرام خاص.
السعر يُعرض بالدولار، والرابط يوجه مباشرة لصفحة محفظتك مفلترة على المجموعة.
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
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
WALLET_ADDRESSES = [
    addr.strip() for addr in os.environ["WALLET_ADDRESSES"].split(",") if addr.strip()
]

ALCHEMY_API_KEY_ROBINHOOD = os.environ["ALCHEMY_API_KEY"]
ALCHEMY_API_KEY_ETHEREUM = os.environ["ALCHEMY_API_KEY_ETHEREUM"]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
COLLECTION_STATS_API = "https://api.opensea.io/api/v2/collections"

LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
HOLDINGS_REFRESH_INTERVAL = 15 * 60
DIGEST_INTERVAL = 30 * 60
ACTIVITY_TIMEOUT = 30 * 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("resale-watcher")

CHAIN_CONFIGS = {
    "robinhood": {
        "stream_chain_name": "robinhood",
        "nft_api_base": f"https://robinhood-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY_ROBINHOOD}",
        "opensea_chain_slug": "robinhood",
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "nft_api_base": f"https://eth-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY_ETHEREUM}",
        "opensea_chain_slug": "ethereum",
    },
}
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

holdings: dict[str, dict] = {}
active_sales: dict[str, float] = {}
_floor_price_cache: dict[str, tuple[float, float]] = {}

_diagnostic_printed = False

_eth_price_cache = {"value": None, "ts": 0}


def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < 300):
        return _eth_price_cache["value"]
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8,
        )
        price = resp.json()["ethereum"]["usd"]
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except Exception as e:
        log.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0


# ---------------------------------------------------------------------------
# جلب مقتنياتك من كل المحافظ × كل الشبكات
# ---------------------------------------------------------------------------

def fetch_holdings_for_wallet(wallet: str, nft_api_base: str) -> list[dict]:
    all_nfts = []
    page_key = None
    try:
        while True:
            params = {"owner": wallet, "pageSize": 100, "withMetadata": "false"}
            if page_key:
                params["pageKey"] = page_key
            resp = requests.get(f"{nft_api_base}/getNFTsForOwner", params=params, timeout=15)
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


def slug_from_nft(contract_address: str, token_id: str, opensea_chain_slug: str) -> str | None:
    try:
        resp = requests.get(
            f"https://api.opensea.io/api/v2/chain/{opensea_chain_slug}/contract/{contract_address}/nfts/{token_id}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code == 429:
            time.sleep(2)
            resp = requests.get(
                f"https://api.opensea.io/api/v2/chain/{opensea_chain_slug}/contract/{contract_address}/nfts/{token_id}",
                headers={"x-api-key": OPENSEA_API_KEY},
                timeout=10,
            )
        if resp.status_code != 200:
            log.warning(f"[OpenSea NFT] HTTP {resp.status_code} لعقد {contract_address} توكن {token_id}")
            return None
        nft_data = resp.json().get("nft") or {}
        slug = nft_data.get("collection")
        if not slug:
            log.warning(f"[OpenSea NFT] لا يوجد حقل 'collection' بالرد لعقد {contract_address}")
        return slug
    except Exception as e:
        log.warning(f"[OpenSea NFT] خطأ لعقد {contract_address}: {e}")
        return None


async def refresh_holdings():
    global _diagnostic_printed

    new_by_contract: dict[tuple, dict] = {}

    for chain_key, cfg in CHAIN_CONFIGS.items():
        for wallet in WALLET_ADDRESSES:
            nfts = await asyncio.to_thread(fetch_holdings_for_wallet, wallet, cfg["nft_api_base"])
            log.info(f"[مقتنيات] محفظة {wallet[:10]}... على {chain_key}: {len(nfts)} قطعة.")

            if nfts and not _diagnostic_printed:
                log.info(f"[تشخيص] أول عنصر خام: {json.dumps(nfts[0], ensure_ascii=False)[:800]}")
                _diagnostic_printed = True

            for nft in nfts:
                contract_address = nft.get("contractAddress")
                token_id = nft.get("tokenId")
                if not contract_address or token_id is None:
                    continue
                key = (chain_key, contract_address.lower())
                if key not in new_by_contract:
                    new_by_contract[key] = {
                        "count": 0, "contract": contract_address,
                        "chain_key": chain_key, "sample_token_id": token_id,
                        "owner_wallet": wallet,
                    }
                new_by_contract[key]["count"] += 1

    log.info(f"[تشخيص] عدد العقود الفريدة المجمّعة بعد الفلترة: {len(new_by_contract)}")

    result: dict[str, dict] = {}
    for (chain_key, _addr), entry in new_by_contract.items():
        opensea_chain_slug = CHAIN_CONFIGS[chain_key]["opensea_chain_slug"]
        slug = await asyncio.to_thread(
            slug_from_nft, entry["contract"], entry["sample_token_id"], opensea_chain_slug
        )
        await asyncio.sleep(0.3)
        if not slug:
            continue
        result[slug] = {
            "count": entry["count"],
            "contract": entry["contract"],
            "chain_key": chain_key,
            "owner_wallet": entry["owner_wallet"],
        }

    holdings.clear()
    holdings.update(result)
    per_chain = {}
    for h in holdings.values():
        per_chain[h["chain_key"]] = per_chain.get(h["chain_key"], 0) + 1
    log.info(f"[مقتنيات] تحديث: {len(holdings)} مجموعة — توزيع: {per_chain}")


async def holdings_refresh_loop():
    while True:
        try:
            await refresh_holdings()
        except Exception as e:
            log.error(f"[مقتنيات] خطأ أثناء التحديث: {e}")
        await asyncio.sleep(HOLDINGS_REFRESH_INTERVAL)


# ---------------------------------------------------------------------------
# السعر الأرضي
# ---------------------------------------------------------------------------

def fetch_floor_price(slug: str) -> float | None:
    now = time.time()
    cached = _floor_price_cache.get(slug)
    if cached and (now - cached[1] < 120):
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
    eth_price_usd = get_eth_price_usd()
    lines = [
        "🔥 <b>نشاط بيع على مقتنياتك</b>",
        f"🕐 {now}",
        "━━━━━━━━━━━━━━━━━━",
    ]
    for slug in active_sales:
        entry = holdings.get(slug)
        if not entry:
            continue
        chain_label = "Robinhood Chain" if entry["chain_key"] == "robinhood" else "Ethereum"
        floor_eth = fetch_floor_price(slug)
        if floor_eth is not None:
            floor_usd = floor_eth * eth_price_usd
            floor_line = f"${floor_usd:,.2f} ({floor_eth:.4f} ETH)"
        else:
            floor_line = "غير متاح حاليًا"
        url = f"https://opensea.io/{entry['owner_wallet']}?collectionSlugs={slug}"
        lines.append(
            f"\n💎 <b>{slug}</b> ({chain_label})\n"
            f"📦 لديك: {entry['count']} قطعة\n"
            f"🏷️ السعر الأرضي الحالي: {floor_line}\n"
            f"🔗 {url}"
        )
    lines.append("\n━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


async def digest_loop():
    while True:
        await asyncio.sleep(DIGEST_INTERVAL)
        now = time.time()
        for slug in list(active_sales.keys()):
            if now - active_sales[slug] > ACTIVITY_TIMEOUT:
                active_sales.pop(slug, None)

        if active_sales:
            enqueue_message(build_digest_message())
            log.info(f"[ملخص] أُرسلت رسالة مجمعة لـ {len(active_sales)} مجموعة نشطة.")


# ---------------------------------------------------------------------------
# الاتصال بـ OpenSea Stream
# ---------------------------------------------------------------------------

async def listen_opensea():
    msg_ref = 0
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"متصل بـ OpenSea Stream — يراقب: {list(CHAIN_CONFIGS.keys())}")
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
                    stream_chain_name = (item.get("chain", {}) or {}).get("name", "")

                    if stream_chain_name not in STREAM_NAME_TO_CHAIN_KEY:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug or slug not in holdings:
                        continue

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
        f"✅ نظام مراقبة نشاط البيع اشتغل — يراقب {len(WALLET_ADDRESSES)} محفظة على "
        f"{', '.join(CHAIN_CONFIGS.keys())}.\nجاري جلب المقتنيات الأولية..."
    )
    await refresh_holdings()
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
