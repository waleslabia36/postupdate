# -*- coding: utf-8 -*-
"""
CRYPTO BARTA — সম্পূর্ণ ফ্রি ও স্বয়ংক্রিয় ক্রিপ্টো নিউজ টেলিগ্রাম বট
======================================================================
এই স্ক্রিপ্টটি সম্পূর্ণভাবে ফ্রি রিসোর্স দিয়ে চলে — কোনো ক্রেডিট কার্ড বা
পেইড বিলিং লাগবে না।

কাজের ধাপ (Pipeline):
  ০. প্রথমবার চালু হলে ৫টি স্যাম্পল/টেস্ট নিউজ পাঠিয়ে পুরো পাইপলাইন যাচাই
     করে নেয় (মিডিয়া হ্যান্ডলিং ও Pollinations.ai ছবি জেনারেশন সহ)।
  ১. RSS ফিড ও X/Twitter (Nitter RSS) থেকে ব্রেকিং ক্রিপ্টো নিউজ সংগ্রহ করা হয়।
  ২. Gemini "gemini-2.5-flash" মডেল (ফ্রি) দিয়ে খবরটি পড়ে ১০০-১৫০ অক্ষরের
     একটি বাংলা ব্রেকিং নিউজ হেডলাইন এবং প্রাসঙ্গিক ইমোজি তৈরি করা হয়।
  ৩. SQLite ডাটাবেসে লিংক সেভ রেখে ডুপ্লিকেট আটকানো হয় + TF-IDF cosine
     similarity দিয়ে একই ধরনের (৭৫%+ মিল) খবর বাদ দেওয়া হয়।
  ৪. খবরে ছবি থাকলে সেটি পাঠানো হয়; না থাকলে Pollinations.ai (সম্পূর্ণ ফ্রি,
     কোনো API key লাগে না) দিয়ে নিরাপদ/টেক্সট-বিহীন ছবি বানিয়ে পাঠানো হয়।
  ৫. টেলিগ্রাম চ্যানেলে নির্দিষ্ট ফরম্যাটে পোস্ট করে, তারপর আবার লুপ শুরু হয়
     (while True) — এভাবে ২৪/৭ চলতে থাকে।

সব সিক্রেট/টোকেন os.getenv() দিয়ে পড়া হয় — কোনো কিছুই কোডে হার্ডকোড করা নেই।
"""

import os
import re
import io
import json
import time
import sqlite3
import logging
import hashlib
import urllib.parse
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from datetime import datetime, timezone

import requests
import feedparser
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

try:
    from google import genai
except ImportError:  # google-genai লাইব্রেরি ইনস্টল না থাকলে
    genai = None


# ---------------------------------------------------------------------------
# লগিং সেটআপ — Railway এর Logs ট্যাবে এগুলো দেখা যাবে
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("crypto-barta")


# ---------------------------------------------------------------------------
# কনফিগারেশন — সব ভ্যালু Environment Variable থেকে আসবে (কোনো হার্ডকোড নেই)
# ---------------------------------------------------------------------------
def env_list(name: str, default: str = "") -> List[str]:
    """কমা দিয়ে আলাদা করা এনভায়রনমেন্ট ভ্যারিয়েবলকে লিস্টে রূপান্তর করে।"""
    raw = os.getenv(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]


class Config:
    # ---- আবশ্যক (Required) — এই ৩টি ছাড়া বট চলবে না ----
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

    # ---- AI মডেল ----
    GEMINI_TEXT_MODEL = os.getenv("GEMINI_TEXT_MODEL", "gemini-3.8-flash")

    # ---- ছবি জেনারেশন (সম্পূর্ণ ফ্রি — Pollinations.ai, কোনো key লাগে না) ----
    ENABLE_AI_IMAGE = os.getenv("ENABLE_AI_IMAGE", "true").lower() == "true"
    POLLINATIONS_BASE = os.getenv(
        "POLLINATIONS_BASE", "https://image.pollinations.ai/prompt"
    )
    POLLINATIONS_WIDTH = os.getenv("POLLINATIONS_WIDTH", "1024")
    POLLINATIONS_HEIGHT = os.getenv("POLLINATIONS_HEIGHT", "1024")

    # ---- নিউজ সোর্স (RSS ফিড — সব ফ্রি ও পাবলিক) ----
    RSS_FEEDS = env_list(
        "RSS_FEEDS",
        ",".join(
            [
                "https://cointelegraph.com/rss",
                "https://www.coindesk.com/arc/outboundfeeds/rss/",
                "https://decrypt.co/feed",
            ]
        ),
    )

    # ---- X/Twitter সোর্স (Nitter RSS — ফ্রি, কোনো পেইড Twitter API লাগে না) ----
    ENABLE_TWITTER = os.getenv("ENABLE_TWITTER", "true").lower() == "true"
    NITTER_BASE = os.getenv("NITTER_BASE", "https://nitter.net")
    TWITTER_USERNAMES = env_list("TWITTER_USERNAMES", "tier10k,whale_alert")

    # ---- স্টোরেজ / ডুপ্লিকেট চেক ----
    DB_PATH = os.getenv("DB_PATH", "/data/news.db")
    SIMILARITY_THRESHOLD = float(os.getenv("SIMILARITY_THRESHOLD", "0.75"))
    SIMILARITY_WINDOW = int(os.getenv("SIMILARITY_WINDOW", "50"))

    # ---- প্রথম রানে টেস্ট নিউজ পাঠানো হবে কিনা ----
    ENABLE_SEED_TEST = os.getenv("ENABLE_SEED_TEST", "true").lower() == "true"

    # ---- লুপ কনফিগারেশন ----
    POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "90"))
    POST_DELAY_SECONDS = int(os.getenv("POST_DELAY_SECONDS", "5"))
    MAX_ITEMS_PER_CYCLE = int(os.getenv("MAX_ITEMS_PER_CYCLE", "5"))

    # ---- হেডলাইনের অক্ষরসংখ্যার সীমা ----
    HEADLINE_MIN_CHARS = 100
    HEADLINE_MAX_CHARS = 150

    FOLLOW_CHANNEL_URL = "https://t.me/cryptobartalove1"


def validate_config():
    """প্রয়োজনীয় Environment Variable মিসিং থাকলে বট শুরুতেই বন্ধ হয়ে যাবে,
    যাতে ভুল কনফিগারেশনে চুপচাপ ফেইল না করে।"""
    missing = []
    if not Config.TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not Config.TELEGRAM_CHANNEL_ID:
        missing.append("TELEGRAM_CHANNEL_ID")
    if not Config.GEMINI_API_KEY:
        missing.append("GEMINI_API_KEY")
    if missing:
        raise RuntimeError(
            f"এই Environment Variable(গুলো) মিসিং: {', '.join(missing)}। "
            "Railway -> Variables এ গিয়ে এগুলো সেট করুন।"
        )
    if genai is None:
        raise RuntimeError(
            "google-genai প্যাকেজ ইনস্টল নেই। requirements.txt চেক করুন।"
        )


# ---------------------------------------------------------------------------
# ডেটা মডেল — একটি নিউজ/টুইট আইটেমকে প্রতিনিধিত্ব করে
# ---------------------------------------------------------------------------
@dataclass
class NewsItem:
    source_name: str
    source_type: str  # "rss" / "twitter" / "seed"
    title: str
    summary: str
    link: str
    images: List[str] = field(default_factory=list)
    published: Optional[str] = None

    @property
    def link_hash(self) -> str:
        """লিংকের SHA-256 হ্যাশ — ডাটাবেসে ডুপ্লিকেট চেক করতে ব্যবহৃত হয়।"""
        return hashlib.sha256(self.link.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# SQLite ডাটাবেস — পোস্ট করা লিংক, হেডলাইন এবং বট-এর মেটা তথ্য সংরক্ষণ করে
# ---------------------------------------------------------------------------
def get_db() -> sqlite3.Connection:
    """ডাটাবেস কানেকশন তৈরি করে ও টেবিল না থাকলে বানিয়ে দেয়।
    Railway তে DB_PATH একটি Volume-এর ভেতরে রাখলে রিস্টার্ট হলেও ডাটা থেকে যাবে।"""
    db_dir = os.path.dirname(Config.DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(Config.DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS posted_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link_hash TEXT UNIQUE NOT NULL,
            link TEXT NOT NULL,
            headline_bn TEXT NOT NULL,
            source TEXT,
            posted INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
        """
    )
    # bot_meta টেবিলটি বট সম্পর্কিত ছোটখাটো "ফ্ল্যাগ" রাখতে ব্যবহৃত হয়,
    # যেমন — "প্রথমবার টেস্ট নিউজ পাঠানো হয়ে গেছে কিনা" সেটা মনে রাখা।
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    conn.commit()
    return conn


def is_link_seen(conn: sqlite3.Connection, link_hash: str) -> bool:
    """এই লিংক আগে কখনো প্রসেস করা হয়েছে কিনা চেক করে (পোস্ট হোক বা বাদ পড়ুক)।"""
    cur = conn.execute("SELECT 1 FROM posted_items WHERE link_hash=?", (link_hash,))
    return cur.fetchone() is not None


def get_recent_headlines(conn: sqlite3.Connection, limit: int) -> List[str]:
    """Similarity চেকের জন্য সর্বশেষ পোস্ট হওয়া হেডলাইনগুলো নিয়ে আসে।"""
    cur = conn.execute(
        "SELECT headline_bn FROM posted_items WHERE posted=1 ORDER BY id DESC LIMIT ?",
        (limit,),
    )
    return [row[0] for row in cur.fetchall()]


def save_item_record(
    conn: sqlite3.Connection, item: NewsItem, headline_bn: str, posted: bool
):
    """প্রতিটি প্রসেস করা আইটেম ডাটাবেসে সেভ করে — posted=0 মানে ডুপ্লিকেট
    হওয়ায় বাদ দেওয়া হয়েছে, কিন্তু পরের সাইকেলে আবার প্রসেস হবে না।"""
    conn.execute(
        """INSERT OR IGNORE INTO posted_items
           (link_hash, link, headline_bn, source, posted, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            item.link_hash,
            item.link,
            headline_bn,
            item.source_name,
            1 if posted else 0,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()


def is_seed_done(conn: sqlite3.Connection) -> bool:
    """৫টি টেস্ট নিউজ আগে একবার পাঠানো হয়ে গেছে কিনা তা bot_meta টেবিল
    থেকে চেক করে। এটি সত্যি হলে বট আর কখনো টেস্ট নিউজ পাঠাবে না।"""
    cur = conn.execute("SELECT value FROM bot_meta WHERE key='seed_completed'")
    row = cur.fetchone()
    return row is not None and row[0] == "true"


def mark_seed_done(conn: sqlite3.Connection):
    """টেস্ট নিউজ পাঠানো শেষ হলে এই ফ্ল্যাগটি ডাটাবেসে স্থায়ীভাবে সেভ করে
    রাখে, যাতে Railway রিস্টার্ট হলেও আবার টেস্ট নিউজ না পাঠায়।"""
    conn.execute(
        "INSERT INTO bot_meta (key, value) VALUES ('seed_completed', 'true') "
        "ON CONFLICT(key) DO UPDATE SET value='true'"
    )
    conn.commit()


# ---------------------------------------------------------------------------
# ডুপ্লিকেট/সাদৃশ্য নির্ণয় — TF-IDF cosine similarity
# ---------------------------------------------------------------------------
def is_duplicate_by_similarity(new_headline: str, recent_headlines: List[str]) -> bool:
    """নতুন হেডলাইনকে সাম্প্রতিক ৫০টি হেডলাইনের সাথে তুলনা করে।
    ৭৫%-এর বেশি মিল থাকলে এটিকে ডুপ্লিকেট ধরা হয় ও পোস্ট করা হয় না।"""
    if not recent_headlines:
        return False
    corpus = recent_headlines + [new_headline]
    try:
        vectorizer = TfidfVectorizer()
        tfidf = vectorizer.fit_transform(corpus)
        sims = cosine_similarity(tfidf[-1], tfidf[:-1])[0]
        max_sim = float(sims.max()) if len(sims) else 0.0
    except ValueError:
        return False
    if max_sim > Config.SIMILARITY_THRESHOLD:
        log.info(f"একই ধরনের খবর পাওয়া গেছে (similarity={max_sim:.2f}) — বাদ দেওয়া হচ্ছে")
        return True
    return False


# ---------------------------------------------------------------------------
# ৫টি সিড/মক (টেস্ট) নিউজ আইটেম — প্রথম রানে পুরো পাইপলাইন যাচাই করতে
# ---------------------------------------------------------------------------
def get_seed_news_items() -> List[NewsItem]:
    """৫টি স্যাম্পল ক্রিপ্টো নিউজ রিটার্ন করে, যা দিয়ে বটের সম্পূর্ণ পাইপলাইন
    (হেডলাইন জেনারেশন, ইমোজি নির্বাচন, ছবি হ্যান্ডলিং, টেলিগ্রাম পোস্টিং)
    যাচাই করা যায় — আসল RSS/Twitter এর জন্য অপেক্ষা না করেই।

    গঠন:
      - ২টি আইটেমে আসল ছবির URL আছে (১টি একক ছবি + ১টি ডুয়াল-ছবি অ্যালবাম)
        যাতে বিদ্যমান মিডিয়া হ্যান্ডলিং যাচাই করা যায়।
      - ৩টি আইটেমে কোনো ছবি নেই (শুধু টেক্সট) যাতে Pollinations.ai
        স্বয়ংক্রিয়ভাবে প্রাসঙ্গিক ছবি তৈরি করে কিনা তা যাচাই করা যায়।
    """
    return [
        # --- ১) একক ছবিসহ — বুলিশ/সার্জ খবর ---
        NewsItem(
            source_name="Seed Test",
            source_type="seed",
            title="Bitcoin surges past new all-time high",
            summary=(
                "Bitcoin price surged to a new all-time high today as institutional "
                "demand and ETF inflows continued to push the market higher, "
                "analysts say the rally could continue into next month."
            ),
            link="https://example.com/seed-test-1-bitcoin-ath",
            images=[
                "https://upload.wikimedia.org/wikipedia/commons/thumb/4/46/Bitcoin.svg/1200px-Bitcoin.svg.png"
            ],
        ),
        # --- ২) দুটি ছবিসহ (অ্যালবাম টেস্ট) — ব্রেকিং/হ্যাক খবর ---
        NewsItem(
            source_name="Seed Test",
            source_type="seed",
            title="Major crypto exchange suffers security breach",
            summary=(
                "A major cryptocurrency exchange has confirmed a security breach "
                "resulting in the loss of user funds. The exchange has paused "
                "withdrawals while investigating the incident with blockchain "
                "security firms."
            ),
            link="https://example.com/seed-test-2-exchange-hack",
            images=[
                "https://upload.wikimedia.org/wikipedia/commons/thumb/4/46/Bitcoin.svg/1200px-Bitcoin.svg.png",
                "https://upload.wikimedia.org/wikipedia/commons/thumb/0/05/Ethereum_logo_2014.svg/1200px-Ethereum_logo_2014.svg.png",
            ],
        ),
        # --- ৩) কোনো ছবি নেই — দেশ-ভিত্তিক/নিয়ন্ত্রক খবর (US SEC) ---
        NewsItem(
            source_name="Seed Test",
            source_type="seed",
            title="US SEC delays decision on new spot crypto ETF applications",
            summary=(
                "The United States Securities and Exchange Commission (SEC) has "
                "delayed its decision on several pending spot crypto ETF "
                "applications, citing the need for further public comment and "
                "review before making a final ruling."
            ),
            link="https://example.com/seed-test-3-sec-etf-delay",
            images=[],
        ),
        # --- ৪) কোনো ছবি নেই — দেশ-ভিত্তিক/বুলিশ খবর (El Salvador) ---
        NewsItem(
            source_name="Seed Test",
            source_type="seed",
            title="El Salvador adds more Bitcoin to its national reserves",
            summary=(
                "El Salvador's government announced it has purchased additional "
                "Bitcoin for its national treasury, continuing its strategy of "
                "accumulating BTC as part of its official reserves policy."
            ),
            link="https://example.com/seed-test-4-el-salvador-btc",
            images=[],
        ),
        # --- ৫) কোনো ছবি নেই — পার্টনারশিপ/লিস্টিং খবর ---
        NewsItem(
            source_name="Seed Test",
            source_type="seed",
            title="Leading crypto exchange announces partnership with major bank",
            summary=(
                "A leading global cryptocurrency exchange has announced a new "
                "partnership with a major international bank to offer crypto "
                "custody and trading services to institutional clients."
            ),
            link="https://example.com/seed-test-5-exchange-bank-partnership",
            images=[],
        ),
    ]


def run_seed_test(conn: sqlite3.Connection):
    """৫টি টেস্ট নিউজ একে একে পুরো পাইপলাইনে (হেডলাইন -> ছবি -> পোস্ট) চালায়,
    যাতে ডেপ্লয় করার সাথে সাথেই বটের সব ফিচার নিজের চোখে যাচাই করা যায়।"""
    log.info("=" * 60)
    log.info("প্রথম রান শনাক্ত হয়েছে — ৫টি টেস্ট নিউজ পাঠানো শুরু হচ্ছে...")
    log.info("=" * 60)
    for idx, item in enumerate(get_seed_news_items(), start=1):
        log.info(f"[টেস্ট {idx}/5] প্রসেস করা হচ্ছে: {item.title}")
        try:
            process_item(conn, item)
        except Exception as e:
            log.error(f"[টেস্ট {idx}/5] ব্যর্থ হয়েছে: {e}")
    log.info("টেস্ট নিউজ পাঠানো সম্পন্ন হয়েছে। এখন থেকে বট স্বাভাবিক RSS/Twitter মোডে চলবে।")


# ---------------------------------------------------------------------------
# ফেচার — RSS নিউজ ও Nitter (X/Twitter) RSS
# ---------------------------------------------------------------------------
def clean_html(raw_html: str) -> str:
    """HTML ট্যাগ সরিয়ে শুধু টেক্সট বের করে আনে।"""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(separator=" ", strip=True)


def extract_images_from_entry(entry) -> List[str]:
    """RSS এন্ট্রি থেকে সর্বোচ্চ ২টি ছবির URL বের করার চেষ্টা করে
    (media:content, media:thumbnail, enclosure, অথবা HTML এর ভেতরের <img>)।"""
    images: List[str] = []

    for m in getattr(entry, "media_content", []) or []:
        url = m.get("url")
        if url and url not in images:
            images.append(url)

    for m in getattr(entry, "media_thumbnail", []) or []:
        url = m.get("url")
        if url and url not in images:
            images.append(url)

    for link in getattr(entry, "links", []) or []:
        if link.get("rel") == "enclosure" and str(link.get("type", "")).startswith("image"):
            url = link.get("href")
            if url and url not in images:
                images.append(url)

    html_blob = ""
    if hasattr(entry, "summary"):
        html_blob += entry.summary or ""
    for c in getattr(entry, "content", []) or []:
        html_blob += c.get("value", "") or ""
    if html_blob:
        soup = BeautifulSoup(html_blob, "html.parser")
        for img in soup.find_all("img"):
            src = img.get("src")
            if src and src not in images:
                images.append(src)

    return images[:2]


def fetch_rss_news() -> List[NewsItem]:
    """সব RSS ফিড থেকে সর্বশেষ খবরগুলো সংগ্রহ করে।"""
    items: List[NewsItem] = []
    for feed_url in Config.RSS_FEEDS:
        try:
            parsed = feedparser.parse(feed_url)
            source_name = parsed.feed.get("title", feed_url) if parsed.feed else feed_url
            for entry in parsed.entries[:10]:
                link = entry.get("link")
                title = entry.get("title", "")
                if not link or not title:
                    continue
                summary = clean_html(entry.get("summary", "") or entry.get("description", ""))
                items.append(
                    NewsItem(
                        source_name=source_name,
                        source_type="rss",
                        title=title,
                        summary=summary[:600],
                        link=link,
                        images=extract_images_from_entry(entry),
                        published=entry.get("published"),
                    )
                )
        except Exception as e:
            log.warning(f"RSS ফিড ফেচ করতে ব্যর্থ {feed_url}: {e}")
    return items


def fetch_twitter_news() -> List[NewsItem]:
    """পাবলিক Nitter RSS ইনস্ট্যান্স থেকে টুইট সংগ্রহ করে (সম্পূর্ণ ফ্রি,
    কোনো Twitter API key লাগে না)।

    নোট: পাবলিক Nitter ইনস্ট্যান্সগুলো মাঝে মাঝে ডাউন থাকে। কাজ না করলে
    NITTER_BASE পরিবর্তন করুন অথবা ENABLE_TWITTER=false সেট করে শুধু RSS দিয়ে চালান।
    """
    if not Config.ENABLE_TWITTER:
        return []
    items: List[NewsItem] = []
    for username in Config.TWITTER_USERNAMES:
        feed_url = f"{Config.NITTER_BASE.rstrip('/')}/{username}/rss"
        try:
            parsed = feedparser.parse(feed_url)
            if not parsed.entries:
                log.warning(f"@{username} এর জন্য Nitter থেকে কিছু পাওয়া যায়নি (ইনস্ট্যান্স ডাউন থাকতে পারে)")
                continue
            for entry in parsed.entries[:5]:
                link = entry.get("link")
                if not link:
                    continue
                title = clean_html(entry.get("title", ""))
                summary = clean_html(entry.get("description", ""))
                items.append(
                    NewsItem(
                        source_name=f"@{username}",
                        source_type="twitter",
                        title=title,
                        summary=summary[:600],
                        link=link,
                        images=extract_images_from_entry(entry),
                        published=entry.get("published"),
                    )
                )
        except Exception as e:
            log.warning(f"@{username} এর Nitter ফিড ফেচ করতে ব্যর্থ: {e}")
    return items


# ---------------------------------------------------------------------------
# Gemini (ফ্রি) দিয়ে বাংলা হেডলাইন + প্রাসঙ্গিক ইমোজি + ছবির প্রম্পট তৈরি
# ---------------------------------------------------------------------------
_genai_client = None

DEFAULT_EMOJI = "🚨"  # ইমোজি নির্বাচন ব্যর্থ হলে এই ডিফল্ট ইমোজি ব্যবহার হবে


def get_genai_client():
    """Gemini ক্লায়েন্ট একবারই তৈরি করে পুনরায় ব্যবহার করে (গতি বাড়ানোর জন্য)।"""
    global _genai_client
    if _genai_client is None:
        _genai_client = genai.Client(api_key=Config.GEMINI_API_KEY)
    return _genai_client


# হেডলাইন + ইমোজি তৈরির জন্য প্রম্পট টেমপ্লেট। মডেলকে JSON ফরম্যাটে উত্তর
# দিতে বলা হয়েছে যাতে ইমোজি ও হেডলাইন আলাদাভাবে নির্ভরযোগ্যভাবে বের করা যায়।
HEADLINE_PROMPT_TEMPLATE = """You are a professional Bengali crypto news editor for a Telegram breaking-news channel.

Read the news content below and produce a JSON object with two fields:

1. "emoji": ONE or TWO emoji characters (as a single string, no spaces) chosen
   intelligently based on the context of the news:
   - Breaking / emergency / hack / exploit / crash news -> use 🚨 or ⚠️ or 💥
   - Bullish / price surge / all-time-high news -> use 🚀 or 📈 or 🟢
   - Bearish / regulatory ban / negative news -> use 📉 or 🔴
   - General listing / partnership / announcement news -> use 📢 or 🤝 or 💎
   - If the news is specific to a particular country (e.g. USA, UAE, El Salvador,
     India, UK, Japan, South Korea, China, etc.), APPEND that country's flag
     emoji right after the category emoji (example: "🚨🇺🇸", "🚀🇸🇻", "📉🇮🇳").
   - If no specific country is mentioned, use only the category emoji (no flag).

2. "headline": ONE single-sentence, high-impact BREAKING NEWS headline written
   in Bengali (বাংলা).

STRICT RULES FOR "headline":
- Bengali text ONLY. No quotes, no English words, no hashtags, and NO emoji
  inside this field (the emoji goes only in the separate "emoji" field above).
- Length MUST be between {min_c} and {max_c} characters (including spaces).
- Sound urgent and newsworthy, appropriate for a breaking-news alert.
- Base it strictly on the facts given below - never invent numbers or facts.

Respond with ONLY a valid JSON object in this exact shape, nothing else,
no markdown code fences, no explanation:
{{"emoji": "...", "headline": "..."}}

CONTENT:
Title: {title}
Details: {summary}
"""


def _parse_headline_json(raw_text: str) -> Optional[dict]:
    """Gemini এর উত্তর থেকে JSON বের করে আনে (মাঝে মাঝে মডেল ```json ... ```
    কোড ফেন্স যোগ করে দেয়, সেটা এখানে পরিষ্কার করা হয়)।"""
    cleaned = raw_text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.IGNORECASE)
    try:
        return json.loads(cleaned)
    except (json.JSONDecodeError, TypeError):
        return None


def _sanitize_emoji(emoji: str) -> str:
    """ইমোজি ফিল্ডে ভুলবশত কোনো ইংরেজি/বাংলা অক্ষর চলে এলে সেটাকে বাতিল করে
    ডিফল্ট ইমোজি ব্যবহার করে — এতে ক্যাপশনে অপ্রত্যাশিত টেক্সট আসবে না।"""
    emoji = (emoji or "").strip()
    if not emoji or len(emoji) > 8:
        return DEFAULT_EMOJI
    if re.search(r"[A-Za-z\u0980-\u09FF]", emoji):  # ইংরেজি বা বাংলা অক্ষর থাকলে
        return DEFAULT_EMOJI
    return emoji


def generate_bengali_headline(item: NewsItem) -> Optional[Tuple[str, str]]:
    """Gemini দিয়ে ১০০-১৫০ অক্ষরের বাংলা হেডলাইন এবং প্রাসঙ্গিক ইমোজি তৈরি
    করে। (emoji, headline) টাপল রিটার্ন করে, অথবা ব্যর্থ হলে None।
    অক্ষরসংখ্যা সীমার মধ্যে না এলে সর্বোচ্চ ৩ বার চেষ্টা করে।"""
    client = get_genai_client()
    prompt = HEADLINE_PROMPT_TEMPLATE.format(
        min_c=Config.HEADLINE_MIN_CHARS,
        max_c=Config.HEADLINE_MAX_CHARS,
        title=item.title,
        summary=item.summary[:500],
    )
    for attempt in range(3):
        try:
            resp = client.models.generate_content(
                model=Config.GEMINI_TEXT_MODEL,
                contents=prompt,
            )
            data = _parse_headline_json(resp.text or "")
            if not data:
                log.warning(f"JSON পার্স করা যায়নি, আবার চেষ্টা করা হচ্ছে ({attempt + 1}/3)")
                time.sleep(1)
                continue

            headline = str(data.get("headline", "")).strip().strip('"').strip()
            headline = re.sub(r"\s+", " ", headline)
            emoji = _sanitize_emoji(str(data.get("emoji", "")))
            length = len(headline)

            if Config.HEADLINE_MIN_CHARS <= length <= Config.HEADLINE_MAX_CHARS:
                return emoji, headline

            log.info(f"হেডলাইনের দৈর্ঘ্য {length} — সীমার বাইরে, আবার চেষ্টা করা হচ্ছে ({attempt + 1}/3)")
            prompt += (
                f"\n\nYour previous attempt was {length} characters. "
                f"Rewrite it strictly between {Config.HEADLINE_MIN_CHARS} and "
                f"{Config.HEADLINE_MAX_CHARS} characters. Respond with the same JSON shape."
            )
        except Exception as e:
            log.warning(f"Gemini হেডলাইন তৈরিতে ব্যর্থ (চেষ্টা {attempt + 1}): {e}")
            time.sleep(2)
    return None


# Pollinations.ai তে পাঠানো প্রতিটি প্রম্পটের শেষে এই নেগেটিভ/সেফটি গাইডেন্স
# যোগ করা হয় — যাতে ছবিতে কোনো বাংলা টেক্সট, ভাঙাচোরা লেখা বা এলোমেলো অক্ষর
# কখনোই না আসে। কোড থেকে সরাসরি যোগ করা হয় বলে এটি সবসময় নিশ্চিতভাবে কার্যকর
# থাকে, Gemini কী প্রম্পট দিলো তার ওপর নির্ভর করে না।
NEGATIVE_IMAGE_GUIDANCE = (
    "no text, no letters, no words, no captions, no watermark, no Bengali script, "
    "clean background, professional 3d render, cinematic lighting, 4k, high quality"
)


def generate_image_prompt(item: NewsItem) -> str:
    """হেডলাইনের ওপর ভিত্তি করে Pollinations.ai এর জন্য একটি ইংরেজি
    visual concept প্রম্পট তৈরি করে (Gemini দিয়ে, সম্পূর্ণ ফ্রি)।
    কড়াভাবে টেক্সট/অক্ষর-বিহীন এবং শুধু পরিষ্কার ইংরেজি লোগো/সিম্বল
    ব্যবহারের নির্দেশনা দেওয়া হয়েছে।"""
    client = get_genai_client()
    prompt = (
        "Create a short English visual-concept prompt (max 30 words) for an AI "
        "image generator, for a crypto news illustration. Cinematic, "
        "3D-rendered / futuristic finance style, 4k.\n"
        "IMPORTANT CONSTRAINTS:\n"
        "- Do NOT describe or request any text, letters, words, captions, or "
        "Bengali script anywhere in the image.\n"
        "- If a coin symbol/logo (e.g. BTC, ETH, BNB) is relevant, describe it "
        "ONLY as a clean, professional, standard English typographic logo — "
        "never distorted, never random overlaid text.\n"
        "- Keep the background clean and uncluttered.\n\n"
        f"Base the visual concept on this news:\nTitle: {item.title}\n"
        f"Details: {item.summary[:300]}\n"
        "Output ONLY the prompt text, nothing else."
    )
    try:
        resp = client.models.generate_content(model=Config.GEMINI_TEXT_MODEL, contents=prompt)
        text = (resp.text or "").strip()
        if text:
            return text
    except Exception as e:
        log.warning(f"ছবির প্রম্পট তৈরিতে ব্যর্থ: {e}")
    return f"futuristic 3d render of {item.title}, glowing crypto chart, dark background"


def generate_ai_image_bytes(prompt: str) -> Optional[bytes]:
    """সম্পূর্ণ ফ্রি Pollinations.ai ব্যবহার করে ছবি তৈরি করে — কোনো API key,
    সাইনআপ বা ক্রেডিট কার্ড লাগে না। প্রতিটি প্রম্পটের শেষে NEGATIVE_IMAGE_GUIDANCE
    যোগ করে দেওয়া হয় যাতে ছবিতে কখনো বাংলা/ভাঙাচোরা টেক্সট না আসে।"""
    if not Config.ENABLE_AI_IMAGE:
        return None
    try:
        final_prompt = f"{prompt}, {NEGATIVE_IMAGE_GUIDANCE}"
        encoded_prompt = urllib.parse.quote(final_prompt)
        image_url = (
            f"{Config.POLLINATIONS_BASE}/{encoded_prompt}"
            f"?width={Config.POLLINATIONS_WIDTH}&height={Config.POLLINATIONS_HEIGHT}&nologo=true"
        )
        # ছবি তৈরি হতে কিছুটা সময় লাগতে পারে, তাই timeout একটু বেশি রাখা হয়েছে
        resp = requests.get(image_url, timeout=90)
        resp.raise_for_status()
        if resp.headers.get("content-type", "").startswith("image"):
            return resp.content
        log.warning("Pollinations.ai থেকে ছবি আসেনি (ভুল content-type)")
    except Exception as e:
        log.warning(f"Pollinations.ai ছবি তৈরিতে ব্যর্থ: {e}")
    return None


# ---------------------------------------------------------------------------
# টেলিগ্রাম পোস্টিং
# ---------------------------------------------------------------------------
TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"


def tg_url(method: str) -> str:
    return TELEGRAM_API.format(token=Config.TELEGRAM_BOT_TOKEN, method=method)


def build_caption(emoji: str, headline_bn: str, source_link: str) -> str:
    """প্রশ্নে দেওয়া ঠিক লেআউট অনুযায়ী ক্যাপশন তৈরি করে — এখন ইমোজি
    প্রতিটি খবরের কনটেক্সট অনুযায়ী ডাইনামিকভাবে বসানো হয় (আগের মতো
    হার্ডকোড করা 🚨 নয়)।"""
    return (
        f"{emoji} <b>{headline_bn}</b>\n\n"
        f'🌐 <b>Source:</b> <a href="{source_link}">Click Here</a>\n\n'
        f'🔔 <b>Follow:</b> <a href="{Config.FOLLOW_CHANNEL_URL}"><b>CRYPTO BARTA</b></a>'
    )


def send_text_message(caption: str) -> bool:
    """ছবি ছাড়া শুধু টেক্সট পোস্ট করে (যখন কোনো ছবিই পাওয়া যায়নি)।"""
    try:
        resp = requests.post(
            tg_url("sendMessage"),
            data={
                "chat_id": Config.TELEGRAM_CHANNEL_ID,
                "text": caption,
                "parse_mode": "HTML",
            },
            timeout=30,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error(f"sendMessage ব্যর্থ: {e}")
        return False


def send_single_photo(caption: str, image_url: str = None, image_bytes: bytes = None) -> bool:
    """একটি ছবিসহ পোস্ট করে — সোর্স URL অথবা ডাউনলোড করা bytes, দুটোই সাপোর্ট করে।"""
    try:
        data = {
            "chat_id": Config.TELEGRAM_CHANNEL_ID,
            "caption": caption,
            "parse_mode": "HTML",
        }
        if image_bytes:
            files = {"photo": ("image.jpg", io.BytesIO(image_bytes))}
            resp = requests.post(tg_url("sendPhoto"), data=data, files=files, timeout=60)
        else:
            data["photo"] = image_url
            resp = requests.post(tg_url("sendPhoto"), data=data, timeout=60)
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error(f"sendPhoto ব্যর্থ: {e}")
        return False


def send_media_group(caption: str, image_urls: List[str]) -> bool:
    """দুটি ছবি থাকলে অ্যালবাম (sendMediaGroup) আকারে পাঠায়, ক্যাপশন প্রথম ছবিতে থাকে।"""
    try:
        media = []
        for i, url in enumerate(image_urls[:2]):
            entry = {"type": "photo", "media": url}
            if i == 0:
                entry["caption"] = caption
                entry["parse_mode"] = "HTML"
            media.append(entry)
        resp = requests.post(
            tg_url("sendMediaGroup"),
            data={"chat_id": Config.TELEGRAM_CHANNEL_ID, "media": json.dumps(media)},
            timeout=60,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error(f"sendMediaGroup ব্যর্থ: {e}")
        return False


def post_news_item(item: NewsItem, emoji: str, headline_bn: str) -> bool:
    """ছবি আছে কি নেই তার ওপর ভিত্তি করে সঠিক পদ্ধতিতে পোস্ট করে:
    ২টি সোর্স ছবি -> অ্যালবাম | ১টি সোর্স ছবি -> একক ছবি |
    কোনো ছবি নেই -> Pollinations.ai দিয়ে ছবি বানিয়ে পাঠানো | সব ব্যর্থ হলে -> শুধু টেক্সট।"""
    caption = build_caption(emoji, headline_bn, item.link)
    valid_images = [u for u in item.images if u and u.startswith("http")]

    if len(valid_images) >= 2:
        log.info("২টি সোর্স ছবি দিয়ে অ্যালবাম আকারে পোস্ট করা হচ্ছে")
        return send_media_group(caption, valid_images[:2])

    if len(valid_images) == 1:
        log.info("১টি সোর্স ছবি দিয়ে পোস্ট করা হচ্ছে")
        return send_single_photo(caption, image_url=valid_images[0])

    if Config.ENABLE_AI_IMAGE:
        log.info("সোর্সে কোনো ছবি নেই — Pollinations.ai দিয়ে ছবি তৈরি করা হচ্ছে")
        img_prompt = generate_image_prompt(item)
        image_bytes = generate_ai_image_bytes(img_prompt)
        if image_bytes:
            return send_single_photo(caption, image_bytes=image_bytes)

    log.info("কোনো ছবি পাওয়া যায়নি — শুধু টেক্সট আকারে পোস্ট করা হচ্ছে")
    return send_text_message(caption)


# ---------------------------------------------------------------------------
# প্রসেসিং পাইপলাইন
# ---------------------------------------------------------------------------
def process_item(conn: sqlite3.Connection, item: NewsItem):
    """একটি নিউজ আইটেমকে হেডলাইন+ইমোজি তৈরি -> ডুপ্লিকেট চেক -> পোস্ট -> সেভ,
    এই পুরো ধাপে নিয়ে যায়। সিড/টেস্ট নিউজ এবং আসল RSS/Twitter নিউজ —
    দুই ক্ষেত্রেই একই ফাংশন ব্যবহৃত হয়, তাই টেস্ট রান আসল পাইপলাইনটাই
    হুবহু যাচাই করে।"""
    if is_link_seen(conn, item.link_hash):
        return
    if not item.title:
        return

    result = generate_bengali_headline(item)
    if not result:
        log.warning(f"সঠিক দৈর্ঘ্যের হেডলাইন তৈরি করা যায়নি, বাদ দেওয়া হচ্ছে: {item.link}")
        return
    emoji, headline_bn = result

    recent_headlines = get_recent_headlines(conn, Config.SIMILARITY_WINDOW)
    if is_duplicate_by_similarity(headline_bn, recent_headlines):
        log.info(f"একই ধরনের (ডুপ্লিকেট) খবর বাদ দেওয়া হলো: {item.link}")
        save_item_record(conn, item, headline_bn, posted=False)
        return

    success = post_news_item(item, emoji, headline_bn)
    save_item_record(conn, item, headline_bn, posted=success)
    if success:
        log.info(f"পোস্ট হয়েছে: {emoji} {headline_bn[:60]}...")
    time.sleep(Config.POST_DELAY_SECONDS)


def run_cycle(conn: sqlite3.Connection):
    """একবার সব সোর্স থেকে খবর নিয়ে এসে যতগুলো সম্ভব প্রসেস করে।"""
    all_items: List[NewsItem] = []
    all_items.extend(fetch_rss_news())
    all_items.extend(fetch_twitter_news())
    log.info(f"এই সাইকেলে মোট {len(all_items)}টি খবর/টুইট পাওয়া গেছে।")

    processed = 0
    for item in all_items:
        if processed >= Config.MAX_ITEMS_PER_CYCLE:
            break
        if is_link_seen(conn, item.link_hash):
            continue
        try:
            process_item(conn, item)
            processed += 1
        except Exception as e:
            log.error(f"আইটেম প্রসেস করতে সমস্যা হয়েছে {item.link}: {e}")


def main():
    """মূল ফাংশন — কনফিগারেশন যাচাই করে, প্রথম রান হলে ৫টি টেস্ট নিউজ
    পাঠায়, তারপর অনন্তকাল ধরে স্বাভাবিক লুপ চালায়।"""
    validate_config()
    conn = get_db()
    log.info("Crypto Barta বট চালু হয়েছে।")

    # প্রথমবার (অথবা bot_meta রিসেট হলে) ৫টি টেস্ট নিউজ পাঠিয়ে পুরো
    # পাইপলাইন যাচাই করা হয়। এরপর এই ফ্ল্যাগ ডাটাবেসে সেভ থাকায় বট আর
    # কখনো পুনরায় টেস্ট নিউজ পাঠাবে না — এমনকি Railway রিস্টার্ট হলেও।
    if Config.ENABLE_SEED_TEST and not is_seed_done(conn):
        run_seed_test(conn)
        mark_seed_done(conn)

    while True:
        cycle_start = time.time()
        try:
            run_cycle(conn)
        except Exception as e:
            log.error(f"সাইকেল চালাতে সমস্যা হয়েছে: {e}")

        elapsed = time.time() - cycle_start
        sleep_time = max(5, Config.POLL_INTERVAL_SECONDS - int(elapsed))
        log.info(f"সাইকেল শেষ হলো {elapsed:.1f} সেকেন্ডে। {sleep_time} সেকেন্ড অপেক্ষা করা হচ্ছে।")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
