# -*- coding: utf-8 -*-
"""main.py-এর অফলাইন স্মোক-টেস্ট — নেটওয়ার্ক/আসল API ছাড়াই সব কোর লজিক যাচাই।"""
import os
import sys
import io
import types
import tempfile
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import main  # noqa: E402

PASS, FAIL = 0, []


def check(name, cond, extra=""):
    global PASS
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL.append(name)
        print(f"  ❌ {name} {extra}")


print("\n[১] validate_config — env var ছাড়া RuntimeError")
try:
    main.validate_config()
    check("RuntimeError হওয়া উচিত ছিল", False)
except RuntimeError as e:
    missing = all(v in str(e) for v in
                  ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHANNEL_ID", "GEMINI_API_KEY"])
    check("৩টা env var এর নাম মেসেজে আছে", missing, str(e)[:120])

print("\n[২] DB স্কিমা + NewsItem.link_hash")
tmpdb = os.path.join(tempfile.mkdtemp(), "news.db")
main.Config.DB_PATH = tmpdb
conn = main.get_db()
tables = {r[0] for r in conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
check("৫টা টেবিল তৈরি", {"posted_items", "bot_meta", "econ_events",
                          "price_milestones", "milestone_alerts"} <= tables, str(tables))
# Previous release DB had econ_events without pin retry timestamps or actual result.
import sqlite3
legacy = sqlite3.connect(":memory:")
legacy.execute("CREATE TABLE econ_events (event_id TEXT PRIMARY KEY, title TEXT NOT NULL, "
               "country TEXT, event_time TEXT NOT NULL, forecast TEXT, previous TEXT, "
               "t60_sent INTEGER NOT NULL DEFAULT 0, t30_sent INTEGER NOT NULL DEFAULT 0, "
               "result_sent INTEGER NOT NULL DEFAULT 0, pinned_message_id INTEGER, "
               "pre_btc REAL, pre_eth REAL)")
main._init_schema(legacy)
legacy_cols = {r[1] for r in legacy.execute("PRAGMA table_info(econ_events)")}
check("পুরনো event DB-তে pin/retry/actual column migration",
      {"pin_done", "unpin_done", "t60_sent_at", "t30_sent_at", "actual"} <= legacy_cols)
legacy.close()
item = main.NewsItem(
    source_name="cointelegraph.com", source_type="rss", title="BTC pumps",
    summary="Bitcoin rose 5%", link="https://example.com/a",
    images=[], published=datetime.now(timezone.utc))
item2 = main.NewsItem(
    source_name="WatcherGuru", source_type="telegram", title="BTC pumps",
    summary="same story", link="https://example.com/a",   # একই লিংক
    images=[], published=datetime.now(timezone.utc))
check("link_hash একই লিংকে মেলে", item.link_hash == item2.link_hash)
check("link_hash SHA-256 (৬৪ অক্ষর)", len(item.link_hash) == 64)

print("\n[৩] ফ্রেশনেস ফিল্টার")
# নোট: বট চালু হওয়ার ৬০ সেকেন্ড আগের আইটেমও stale — কারণ cutoff =
# max(BOT_START_TIME, now-300s), অর্থাৎ বট স্টার্টের আগেরটা কখনোই নেওয়া হবে না।
fresh = main.NewsItem(source_name="x", source_type="rss", title="t", summary="s",
                      link="https://e.com/f",
                      published=datetime.now(timezone.utc))  # এইমাত্র প্রকাশিত
before_start_60 = main.NewsItem(
    source_name="x", source_type="rss", title="t", summary="s",
    link="https://e.com/f2",
    published=datetime.now(timezone.utc) - timedelta(seconds=60))
old = main.NewsItem(source_name="x", source_type="rss", title="t", summary="s",
                    link="https://e.com/o",
                    published=datetime.now(timezone.utc) - timedelta(seconds=600))
before_start = main.NewsItem(
    source_name="x", source_type="rss", title="t", summary="s",
    link="https://e.com/b",
    published=main.BOT_START_TIME - timedelta(hours=2))
check("এইমাত্র প্রকাশিত আইটেম fresh", main.is_item_fresh(fresh))
check("১০ মিনিট আগের আইটেম stale", not main.is_item_fresh(old))
check("বট স্টার্টের ৬০ সেকেন্ড আগেরটাও stale", not main.is_item_fresh(before_start_60))
check("বট স্টার্টের আগের আইটেম stale (backfill বন্ধ)", not main.is_item_fresh(before_start))

print("\n[৪] Gemini JSON পার্স + রিট্রাই/ফলব্যাক লজিক")
good = ('```json\n{"impact":"HIGH","sentiment":"bullish","headline_bn":"' +
        ("বিটকয়েন তে আকস্মিক উত্থান " * 6).strip()[:130] +
        '","emoji":"🚀"}\n```')
p = main._parse_headline_json(good)
check("ফেন্সসহ JSON + impact পার্স", p is not None and p["impact"] == "HIGH"
      and p["sentiment"] == "BULLISH" and 100 <= len(p["headline_bn"]) <= 150, str(p)[:80])
check("নোয়া রেসপন্স → None", main._parse_headline_json("আমি JSON দিই না") is None)
neutral = main._parse_headline_json(
    '{"impact":"HIGH","sentiment":"MAYBE","headline_bn":"পরীক্ষা","emoji":"<b>🚀</b>"}')
check("ভুল sentiment → NEUTRAL; caption builder-এ emoji HTML-escape",
      neutral is not None and neutral["sentiment"] == "NEUTRAL"
      and "&lt;b&gt;" in main.build_news_caption("headline", "NEUTRAL", neutral["emoji"], item))

# generate_bengali_headline: প্রথম রেসপন্স ছোট হেডলাইন, দ্বিতীয়টা ঠিক → ২ চেষ্টা লাগবে
main.Config.AI_CALL_MIN_DELAY = 0
main.Config.AI_CALL_MAX_DELAY = 0
calls = {"n": 0}
short = '{"impact":"HIGH","sentiment":"BULLISH","headline_bn":"খুব ছোট","emoji":"🚀"}'
long_ok = ('{"impact":"HIGH","sentiment":"BEARISH","headline_bn":"' +
           ("ইথেরিয়াম মার্কেটে নেতিবাচক চাপ তৈরি হয়েছে সর্বশেষ চার ঘণ্টায় " * 4)[:130] +
           '","emoji":"📉"}')


def fake_call(model, prompt):
    calls["n"] += 1
    if calls["n"] == 1:
        return short          # length ভুল → rewrite নির্দেশনা যোগ হবে
    return long_ok


orig_call = main._call_gemini
main._call_gemini = fake_call
status, data = main.generate_bengali_headline(item)
main._call_gemini = orig_call
check("HIGH-impact length retry কাজ করে (২ কল)", status == "ok" and calls["n"] == 2
      and data["sentiment"] == "BEARISH" and data["impact"] == "HIGH",
      f"status={status} calls={calls}")

# Neutral/low-impact must be filtered without a headline or extra retry.
main._call_gemini = lambda model, prompt: '{"impact":"LOW","sentiment":"NEUTRAL","headline_bn":"","emoji":"📰"}'
status, data = main.generate_bengali_headline(item)
main._call_gemini = orig_call
check("LOW/NEUTRAL খবর ফিল্টার হয়", status == "filtered" and data["impact"] == "LOW")

# সব API call ব্যর্থ → api_failed
def raise_call(model, prompt):
    raise RuntimeError("429 quota")


main._call_gemini = raise_call
status, data = main.generate_bengali_headline(item)
main._call_gemini = orig_call
check("API ব্যর্থ → api_failed (রিট্রাই হবে)", status == "api_failed" and data is None)

print("\n[৫] ক্যাপশন ফরম্যাট (হুবহু স্পেসিফিকেশন)")
cap = main.build_news_caption("বিটকয়েন $১০০k ছুঁলো", "BULLISH", "🚀", item)
check("HEADLINE বোল্ড", "<b>বিটকয়েন $১০০k ছুঁলো</b>" in cap)
check("MARKET HINT 🟢", "📊 <b>MARKET HINT:</b> <b>BULLISH</b> 🟢" in cap)
check("RSS-এ Source লাইন আছে", '🌐 <b>Source:</b> <a href="https://example.com/a">Click Here</a>' in cap)
check("শেষ লাইন: এক লাইনে Follow CRYPTO UPDATE → tmcryptoupdate",
      '🔔 <b><a href="https://t.me/tmcryptoupdate">Follow CRYPTO UPDATE</a></b>' in cap)
check("পুরনো 'Follow:' (কোলনসহ) ও cryptobartalove1 নেই",
      "Follow:" not in cap and "cryptobartalove1" not in cap)
check("Follow আর CRYPTO UPDATE একই লাইনে (একই <a> ট্যাগে)",
      ">Follow CRYPTO UPDATE</a>" in cap)
tg_item = main.NewsItem(source_name="WatcherGuru", source_type="telegram",
                        title="t", summary="s", link="https://t.me/w/1",
                        published=datetime.now(timezone.utc))
cap_tg = main.build_news_caption("হেডলাইন", "NEUTRAL", "📰", tg_item)
check("Telegram আইটেমে Source লাইন নেই", "🌐" not in cap_tg)

# প্রাইস অ্যালার্ট ক্যাপশন
c_btc = main.build_milestone_caption("BTCUSDT", 80523.45, True, 80500.0, "Binance")
c_eth = main.build_milestone_caption("ETHUSDT", 2650.0, False, 2650.0, "CoinGecko")
check("BTC caption live quote + crossed level + source",
      "BTC $80,523.45 (crossed $80,500) · Binance" in c_btc
      and "[@btc_price](https://t.me/tmmusa73)" in c_btc, c_btc)
check("ETH caption live quote + source",
      "ETH $2,650 · CoinGecko" in c_eth
      and "[@eth_price](https://t.me/tmmusa73)" in c_eth, c_eth)
check("live price ও threshold আলাদা করে দেখায়", "$80,523.45" in c_btc and "$80,500" in c_btc)
check("exact whole-dollar quote-এ অপ্রয়োজনীয় decimal নেই", ".00" not in c_eth)

print("\n[৬] প্রাইস কার্ড রেন্ডার (mock OHLC + ২৪ঘ%)")
fake_candles = [
    (79000.0, 79800.0, 78800.0, 79500.0),   # green
    (79500.0, 79700.0, 78900.0, 79050.0),   # red
    (79050.0, 80400.0, 79000.0, 80200.0),   # green
    (80200.0, 80600.0, 79900.0, 80100.0),   # red
]
main.fetch_recent_candles = lambda s: fake_candles
main.fetch_24hr_change_percent = lambda s: -1.23    # মার্কেট ডাউন → লাল
card = main.generate_price_card_bytes("BTCUSDT", 80500.0, True)
check("PNG bytes তৈরি", card is not None and card[:8] == b"\x89PNG\r\n\x1a\n")
from PIL import Image
im = Image.open(io.BytesIO(card))
check("উচ্চতা 340 + ন্যূনতম চওড়া 1000", im.size[1] == 340 and im.size[0] >= 1000, str(im.size))
check("ফন্ট ক্যাশে এম্বেডেড লোড সফল",
      main._FONT_LOGGED.get("embedded") is True or len(main._FONT_CACHE) > 0)
# বড় দামের ক্ষেত্রে চওড়া বাড়ে
card_wide = main.generate_price_card_bytes("BTCUSDT", 100000.0, False)
im2 = Image.open(io.BytesIO(card_wide))
check("১,০০,০০০ দামে কার্ড চওড়া হয়/overlap এড়ায়", im2.size[0] >= im.size[0], f"{im.size} → {im2.size}")
# ক্যান্ডেল ডেটা না থাকলে placeholder
main.fetch_recent_candles = lambda s: []
card_ph = main.generate_price_card_bytes("ETHUSDT", 2700.0, True)
check("ক্যান্ডেল ফেল হলেও কার্ড তৈরি (placeholder)", card_ph is not None)
main.fetch_recent_candles = lambda s: fake_candles

print("\n[৭] ওয়াটারমার্ক ক্রপ (AI ছবি)")
class FakeResp:
    content = None
    def raise_for_status(self):
        pass
src_img = Image.new("RGB", (512, 512), (30, 30, 60))
buf = io.BytesIO(); src_img.save(buf, format="PNG"); FakeResp.content = buf.getvalue()
orig_get = main.requests.get
main.requests.get = lambda *a, **k: FakeResp()
main.Config.ENABLE_AI_IMAGE = True
ai = main.generate_ai_image_bytes(item, "headline")
main.requests.get = orig_get
im3 = Image.open(io.BytesIO(ai))
check("AI image watermark crop-এর পর ৪:৩ output", im3.size == (512, 384), str(im3.size))

print("\n[৮] মাইলস্টোন লজিক (baseline → ক্রস → কুলডাউন)")
sent_alerts = []
main.send_single_photo = lambda **k: (sent_alerts.append(("photo", k.get("caption"))), True)[1]
main.send_text_message = lambda **k: (sent_alerts.append(("text", k.get("caption"))), True)[1]
main.generate_price_card_bytes = lambda *a, **k: b"\x89PNG\r\n\x1a\nfake"
price_state = {"BTCUSDT": 80123.0, "ETHUSDT": 2601.0}
orig_quote = main.fetch_spot_quote
main.fetch_spot_quote = lambda s: (price_state[s], "Binance")

# (ক) প্রথমবার → শুধু বেসলাইন, অ্যালার্ট নয়
main.check_price_milestone(conn, "BTCUSDT")
row = conn.execute("SELECT last_milestone FROM price_milestones WHERE symbol='BTCUSDT'").fetchone()
check("প্রথম কলে baseline=80000, অ্যালার্ট ০", row and row[0] == 80000.0
      and len(sent_alerts) == 0, str(row) + str(sent_alerts))

# (খ) একই বাকেটে ঘোরাঘুরি → কিছুই নয়
price_state["BTCUSDT"] = 80499.0
main.check_price_milestone(conn, "BTCUSDT")
check("একই বাকেটে কোনো অ্যালার্ট নয়", len(sent_alerts) == 0)

# (গ) 80500 ক্রস → অ্যালার্ট (কার্ড সহ)
price_state["BTCUSDT"] = 80510.0
main.check_price_milestone(conn, "BTCUSDT")
check("ক্রসে ১টা alert-এ actual spot + crossed threshold",
      len(sent_alerts) == 1 and sent_alerts[0][0] == "photo"
      and "BTC $80,510 (crossed $80,500)" in sent_alerts[0][1], str(sent_alerts))
row = conn.execute("SELECT last_milestone FROM price_milestones WHERE symbol='BTCUSDT'").fetchone()
check("বেসলাইন আপডেট 80500", row and row[0] == 80500.0)
n_alerts = conn.execute("SELECT COUNT(*) FROM milestone_alerts").fetchone()[0]
check("কুলডাউন টেবিলে ১ রেকর্ড", n_alerts == 1)

# (ঘ) বাউন্স: 80480 → 80500 আবার ক্রস, কিন্তু ২৪ ঘণ্টা কুলডাউন → স্কিপ
price_state["BTCUSDT"] = 80480.0
main.check_price_milestone(conn, "BTCUSDT")   # 80500 → 80000 (ডাউন ক্রস, নতুন বাকেট)
sent_alerts.clear()
price_state["BTCUSDT"] = 80505.0
main.check_price_milestone(conn, "BTCUSDT")   # 80000 → 80500 (আবার উপরে), কিন্তু কুলডাউন
check("কুলডাউনে ডুপ্লিকেট অ্যালার্ট যায় না", len(sent_alerts) == 0, str(sent_alerts))

# (ঙ) দূরের নতুন মাইলস্টোন (81000) → অ্যালার্ট যাবে
price_state["BTCUSDT"] = 81001.0
main.check_price_milestone(conn, "BTCUSDT")
check("নতুন মাইলস্টোনে অ্যালার্ট যায়", len(sent_alerts) == 1
      and "$81,000" in sent_alerts[0][1], str(sent_alerts))

# (চ) দাম আনা যায় না → ক্র্যাশ নয়
main.fetch_spot_quote = lambda s: None
main.check_price_milestone(conn, "BTCUSDT")
check("দাম None হলে শান্তভাবে স্কিপ", True)
main.fetch_spot_quote = lambda s: (price_state[s], "Binance")

# (ছ) Telegram send fail হলে baseline advance হবে না; পরের poll-এ retry হবে।
main.check_price_milestone(conn, "ETHUSDT")  # initial baseline = 2600
price_state["ETHUSDT"] = 2652.0
main.send_single_photo = lambda **k: False
main.send_text_message = lambda *a, **k: False
main.check_price_milestone(conn, "ETHUSDT")
eth_row = conn.execute("SELECT last_milestone FROM price_milestones WHERE symbol='ETHUSDT'").fetchone()
check("failed alert-এ পুরনো baseline থাকে", eth_row and eth_row[0] == 2600.0)
main.send_text_message = lambda *a, **k: True
main.check_price_milestone(conn, "ETHUSDT")
eth_row = conn.execute("SELECT last_milestone FROM price_milestones WHERE symbol='ETHUSDT'").fetchone()
check("পরের poll-এ সফল alert হলে baseline এগোয়", eth_row and eth_row[0] == 2650.0)
main.fetch_spot_quote = orig_quote

print("\n[৯] সোর্স ছবির fallback")
image_item = main.NewsItem(source_name="x", source_type="rss", title="t", summary="s",
                           link="https://e.com/img", images=["https://e.com/bad", "https://e.com/good"],
                           published=datetime.now(timezone.utc))
image_attempts = []
orig_download = main._download_image_bytes
orig_send_photo = main.send_single_photo
orig_ai = main.generate_ai_image_bytes
main._download_image_bytes = lambda url: (image_attempts.append(url) or
                                           (None if url.endswith("bad") else (b"jpeg", "image/jpeg", "source.jpg")))
main.send_single_photo = lambda **kwargs: True
main.generate_ai_image_bytes = lambda *a, **k: None
image_post_ok = main.post_news_item(image_item, "headline", "BULLISH", "🚀")
main._download_image_bytes, main.send_single_photo, main.generate_ai_image_bytes = orig_download, orig_send_photo, orig_ai
check("প্রথম ছবি খারাপ হলে দ্বিতীয় valid ছবিটি চেষ্টা হয়",
      image_post_ok and image_attempts == ["https://e.com/bad", "https://e.com/good"], str(image_attempts))

print("\n[১০] Scheduled event stages + Bangladesh time")
dt_test = main._parse_event_datetime("2026-10-02T08:30:00-04:00")
check("NFP 08:30 ET → বাংলাদেশ 06:30 PM (DST-aware)", "06:30 PM" in main._fmt_dhaka(dt_test))
event_now = datetime.now(timezone.utc)
event_time = event_now + timedelta(minutes=70)
event = {"event_id": "smoke-event-1", "title": "Non-Farm Employment Change",
          "country": "USD", "event_time": event_time, "forecast": "100K",
          "previous": "90K", "actual": "95K", "impact": "HIGH"}
orig_events = main.fetch_econ_events
orig_pre_analysis = main.analyze_econ_event
orig_result_analysis = main.analyze_econ_result
orig_send_notify = main.send_text_message_notify
orig_send_text = main.send_text_message
orig_pin = main.pin_chat_message
orig_unpin = main.unpin_chat_message
orig_price = main.fetch_spot_price
event_in_feed = {"visible": True}
def fake_events(force_refresh=False):
    return [event] if event_in_feed["visible"] else []
main.fetch_econ_events = fake_events
main.analyze_econ_event = lambda ev: "scenario bullish/bearish"
main.analyze_econ_result = lambda ev, bb, ba, eb, ea: ("observed reaction", (ba-bb)/bb*100, (ea-eb)/eb*100)
prices = {"BTCUSDT": 100000.0, "ETHUSDT": 3000.0}
main.fetch_spot_price = lambda symbol: prices[symbol]
notify_log, normal_log, pins, unpins = [], [], [], []
main.send_text_message_notify = lambda text: (notify_log.append(text) or 9001 + len(notify_log))
main.send_text_message = lambda text, **kw: (normal_log.append(text) or True)
main.pin_chat_message = lambda mid: (pins.append(mid) or True)
main.unpin_chat_message = lambda mid: (unpins.append(mid) or True)
main._econ_cycle(conn, now=event_time - timedelta(minutes=59))
main._econ_cycle(conn, now=event_time - timedelta(minutes=29))
prices.update({"BTCUSDT": 101000.0, "ETHUSDT": 3030.0})
main._econ_cycle(conn, now=event_time - timedelta(minutes=4))
prices.update({"BTCUSDT": 102000.0, "ETHUSDT": 3060.0})
event_in_feed["visible"] = False  # provider may drop past event before analysis finishes
main._econ_cycle(conn, now=event_time + timedelta(minutes=5))
row = conn.execute("SELECT t60_sent,t30_sent,result_sent,pinned_message_id,pin_done,unpin_done "
                   "FROM econ_events WHERE event_id='smoke-event-1'").fetchone()
check("T-60 full alert, T-30 short reminder, result post, pin/unpin complete",
      row and row[0] == 1 and row[1] == 1 and row[2] == 1 and row[4] == 1 and row[5] == 1
      and len(notify_log) == 2 and len(normal_log) == 1 and len(pins) == 1 and len(unpins) == 1,
      str(row))
check("T-30 reminder full analysis পুনরায় পাঠায় না",
      "আর ৩০ মিনিট" in notify_log[1] and "scenario bullish/bearish" not in notify_log[1])
check("post-event alert-এ actual release result থাকে",
      "Actual: <b>95K</b>" in normal_log[0])
(main.fetch_econ_events, main.analyze_econ_event, main.analyze_econ_result,
 main.send_text_message_notify, main.send_text_message, main.pin_chat_message,
 main.unpin_chat_message, main.fetch_spot_price) = (
 orig_events, orig_pre_analysis, orig_result_analysis, orig_send_notify,
 orig_send_text, orig_pin, orig_unpin, orig_price)

print("\n[১১] সেমান্টিক ডুপ্লিকেট (TF-IDF)")
main.Config.DB_PATH = tmpdb
h1 = "বিটকয়েনের দাম হঠাৎ ৫ শতাংশ বেড়ে ৮০ হাজার ডলার ছুঁলো"
h2 = "বিটকয়েনের দাম হঠাৎ ৫ শতাংশ বেড়ে ৮০ হাজার ডলার ছুঁলো"  # হুবহু মিল
h3 = "সলানা নেটওয়ার্কের নতুন আপগ্রেড টেস্টনেটে সফল হয়েছে"
main.record_item(conn, item, h1, posted=1)
conn.commit()
check("হুবহু মিল → ডুপ্লিকেট", main.is_similar_to_recent(conn, h2))
check("ভিন্ন খবর → ডুপ্লিকেট নয়", not main.is_similar_to_recent(conn, h3))

print("\n[১২] লিংক ডুপ্লিকেট + run_cycle লিমিট কাউন্টার")
check("record করা লিংক আবার প্রসেস হবে না", main.link_already_processed(conn, item.link_hash))
check("নতুন লিংক প্রসেসযোগ্য", not main.link_already_processed(conn, "deadbeef" * 8))

print(f"\n{'='*60}\nফলাফল: {PASS}টা পাস, {FAIL and len(FAIL) or 0}টা ফেল")
if FAIL:
    print("ফেল:", FAIL)
    sys.exit(1)
print("সব টেস্ট পাস 🎉")
