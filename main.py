# -*- coding: utf-8 -*-
"""
CRYPTO BARTA — সম্পূর্ণ ফ্রি ও স্বয়ংক্রিয় ক্রিপ্টো নিউজ টেলিগ্রাম বট
======================================================================
এই স্ক্রিপ্টটি সম্পূর্ণভাবে ফ্রি রিসোর্স দিয়ে চলে — কোনো ক্রেডিট কার্ড বা
পেইড বিলিং লাগবে না।

কাজের ধাপ (Pipeline):
  ১. প্রাইমারি সোর্স হিসেবে RSS ফিড (Cointelegraph, CoinDesk, Decrypt) থেকে
     ব্রেকিং ক্রিপ্টো নিউজ সংগ্রহ করা হয় — এগুলো সবচেয়ে স্থিতিশীল ও দ্রুত
     উৎস। সেকেন্ডারি/বেস্ট-এফোর্ট সোর্স হিসেবে X/Twitter (Nitter RSS —
     tier10k, whale_alert) থেকেও খবর সংগ্রহ করা হয়; পাবলিক Nitter
     ইনস্ট্যান্স অস্থির হওয়ায় এটি টাইমআউটসহ ফেচ করা হয় এবং কোনো
     অ্যাকাউন্টের ফিড সাময়িকভাবে না পাওয়া গেলে সেটা গ্রেসফুলি স্কিপ করে
     পুরো সাইকেল চালিয়ে যাওয়া হয়, কখনো আটকে থাকে না। কোনো পোস্টে
     ভিডিও/GIF থাকলে (বা কোনো স্ট্যাটিক ছবি না থাকলে) সেটা কখনোই
     ডাউনলোড/পাঠানো হয় না — বরং ছবি-বিহীন ধরে Pollinations.ai দিয়ে AI ছবি
     জেনারেট করে পাঠানো হয়।
  ২. Gemini "gemini-3.5-flash-lite" মডেল (ফ্রি, দিনে ১৫০০ রিকোয়েস্ট কোটা;
     কোটা/রেট-লিমিটে ব্যর্থ হলে "gemini-3.1-flash-lite" এ স্বয়ংক্রিয়
     ফলব্যাক) দিয়ে খবরটি পড়ে মূল ঘটনার ১০০-১৫০ অক্ষরের একটি বাংলা "কোর
     ইনসিডেন্ট সামারি" হেডলাইন এবং প্রাসঙ্গিক ইমোজি তৈরি করা হয় (জেনেরিক
     টিজার বাক্য নয়)।
  ৩. SQLite ডাটাবেসে লিংক সেভ রেখে ডুপ্লিকেট আটকানো হয় + TF-IDF cosine
     similarity দিয়ে সব সোর্স (RSS/Twitter, যেকোনো অ্যাকাউন্ট) জুড়ে একই
     ধরনের (৭৫%+ মিল) খবর বাদ দেওয়া হয় — একই ঘটনা Decrypt, CoinDesk বা
     আলাদা X অ্যাকাউন্ট থেকে দ্বিতীয়বার এলেও সেটা পোস্ট হয় না।
  ৪. খবরে ছবি থাকলে সেটি পাঠানো হয়; না থাকলে Pollinations.ai (সম্পূর্ণ ফ্রি,
     কোনো API key লাগে না) দিয়ে নিরাপদ/টেক্সট-বিহীন ছবি বানিয়ে পাঠানো হয়।
  ৫. টেলিগ্রাম চ্যানেলে নির্দিষ্ট ফরম্যাটে পোস্ট করে, তারপর আবার লুপ শুরু হয়
     (while True) — এভাবে ২৪/৭ চলতে থাকে।
  ৬. এর পাশাপাশি একটি আলাদা ব্যাকগ্রাউন্ড থ্রেডে BTC/USDT ও ETH/USDT এর
     লাইভ স্পট প্রাইস প্রতি ৩০-৪৫ সেকেন্ডে Binance (ফলব্যাক: CoinGecko)
     পাবলিক API দিয়ে চেক করা হয়। ETH প্রতি $৫০ এবং BTC প্রতি $৫০০ মাইলস্টোন
     অতিক্রম করলে (উপরে ↑ বা নিচে ↓) Pollinations.ai দিয়ে একটি 3D আর্ট
     জেনারেট করে একলাইনের ক্যাপশনসহ চ্যানেলে পোস্ট করা হয়। একই মাইলস্টোনের
     আশেপাশে দাম ঘোরাঘুরি করলে ডুপ্লিকেট অ্যালার্ট যায় না — শেষ ট্রিগার হওয়া
     মাইলস্টোন SQLite তে সেভ থাকে।

শুধুমাত্র ৩টি আবশ্যক ক্রেডেনশিয়াল (TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID,
GEMINI_API_KEY) os.getenv() দিয়ে পড়া হয়। বাকি সব (অপশনাল) সেটিং কোডের
Config ক্লাসেই হার্ডকোড করা — Railway তে এর বাইরে আর কিছু সেট করার দরকার
নেই।
"""

import os
import re
import io
import json
import math
import time
import random
import sqlite3
import logging
import hashlib
import threading
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
    level="INFO",
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("crypto-barta")


# ---------------------------------------------------------------------------
# কনফিগারেশন
# ---------------------------------------------------------------------------
# শুধুমাত্র ৩টি আবশ্যক ক্রেডেনশিয়াল Environment Variable থেকে আসে —
# TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID, GEMINI_API_KEY। বাকি সব
# (অপশনাল) সেটিং সরাসরি নিচে কোডেই হার্ডকোড করা — Railway তে আলাদা করে
# আর কিছু সেট করার দরকার নেই। কোনো সেটিং বদলাতে চাইলে সরাসরি এখানে এসে
# ভ্যালু পাল্টে দিলেই হবে।
class Config:
    # ---- আবশ্যক (Required) — শুধু এই ৩টি Railway এনভায়রনমেন্ট ভ্যারিয়েবল ----
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

    # ---- AI মডেল (হার্ডকোড) ----
    # "gemini-3.5-flash-lite" — ফ্রি কোটা দিনে ১৫০০ রিকোয়েস্ট। প্রাইমারি
    # মডেল কোটা/রেট-লিমিটে ব্যর্থ হলে কোডেই স্বয়ংক্রিয়ভাবে ফলব্যাক মডেল
    # "gemini-3.1-flash-lite" দিয়ে আবার চেষ্টা করা হয়।
    GEMINI_TEXT_MODEL = "gemini-3.5-flash-lite"
    GEMINI_FALLBACK_MODEL = "gemini-3.1-flash-lite"

    # ---- Gemini API কলের মধ্যে রেট-লিমিট বিরতি (হার্ডকোড) ----
    # প্রতিটি Gemini কলের পর ৩-৫ সেকেন্ড এলোমেলো বিরতি দেওয়া হয়, যাতে
    # হঠাৎ অনেকগুলো রিকোয়েস্ট একসাথে গিয়ে বার্স্ট-লিমিটে না পড়ে।
    AI_CALL_MIN_DELAY = 3.0
    AI_CALL_MAX_DELAY = 5.0

    # ---- ছবি জেনারেশন (সম্পূর্ণ ফ্রি — Pollinations.ai, কোনো key লাগে না) ----
    ENABLE_AI_IMAGE = True
    POLLINATIONS_BASE = "https://image.pollinations.ai/prompt"
    POLLINATIONS_WIDTH = "1024"
    POLLINATIONS_HEIGHT = "1024"

    # ---- নিউজ সোর্স (RSS ফিড — সব ফ্রি, পাবলিক ও অত্যন্ত নির্ভরযোগ্য) ----
    # পাবলিক Nitter ইনস্ট্যান্স মাঝে মাঝে ডাউন/অস্থির থাকে বলে Cointelegraph কে
    # আবার সরাসরি তাদের অফিসিয়াল RSS ফিডে ফিরিয়ে আনা হয়েছে — RSS হলো এই
    # তিনটি সোর্সের (Cointelegraph, CoinDesk, Decrypt) সবচেয়ে স্থিতিশীল ও
    # রিয়েল-টাইম উৎস, তাই এগুলোই প্রাইমারি নিউজ সোর্স।
    RSS_FEEDS = [
        "https://cointelegraph.com/rss",
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://decrypt.co/feed",
    ]

    # ---- X/Twitter সোর্স (Nitter RSS — ফ্রি, কোনো পেইড Twitter API লাগে না) ----
    # এটি এখন সেকেন্ডারি/বেস্ট-এফোর্ট সোর্স হিসেবে কাজ করে — Nitter ইনস্ট্যান্স
    # অস্থির হওয়ায়, কোনো একাউন্টের ফিড সাময়িকভাবে না পাওয়া গেলে (টাইমআউট বা
    # এরর) সেটা নিঃশব্দে স্কিপ হয়ে যায় এবং পুরো সাইকেল থেমে থাকে না, লগে শুধু
    # সতর্কবার্তা যায়। Cointelegraph এখন RSS দিয়ে কভার হচ্ছে, তাই Twitter
    # লিস্টে শুধু সেই অ্যাকাউন্টগুলো রাখা হয়েছে যাদের নিজস্ব RSS ফিড নেই।
    ENABLE_TWITTER = True
    NITTER_BASE = "https://nitter.net"
    TWITTER_USERNAMES = ["tier10k", "whale_alert"]
    # Nitter/RSS ফিড আনতে requests এর টাইমআউট (সেকেন্ড) — কোনো ইনস্ট্যান্স
    # সাড়া না দিলে যেন পুরো সাইকেল আটকে না থেকে দ্রুত পরের সোর্সে চলে যায়।
    FEED_FETCH_TIMEOUT_SECONDS = 12

    # ---- স্টোরেজ / ডুপ্লিকেট চেক (হার্ডকোড) ----
    DB_PATH = "/data/news.db"
    # TF-IDF cosine similarity — RSS/Twitter, যেকোনো সোর্স থেকে আসা খবরকে
    # শেষ ৫০টি পোস্ট হওয়া হেডলাইনের সাথে তুলনা করে ৭৫%+ মিল পেলে বাদ দেয়।
    # এই চেক সব সোর্সের জন্য অভিন্ন (cross-source) — নিচে বিস্তারিত ব্যাখ্যা।
    SIMILARITY_THRESHOLD = 0.75
    SIMILARITY_WINDOW = 50

    # ---- সিড/টেস্ট নিউজ ফিচার — সম্পূর্ণ বন্ধ (main() থেকে আর কল হয় না) ----
    ENABLE_SEED_TEST = False

    # ---- লুপ কনফিগারেশন (হার্ডকোড) ----
    POLL_INTERVAL_SECONDS = 90
    POST_DELAY_SECONDS = 5
    MAX_ITEMS_PER_CYCLE = 5

    # ---- হেডলাইনের অক্ষরসংখ্যার সীমা ----
    HEADLINE_MIN_CHARS = 100
    HEADLINE_MAX_CHARS = 150

    FOLLOW_CHANNEL_URL = "https://t.me/cryptobartalove1"

    # ---- BTC/ETH প্রাইস মাইলস্টোন অ্যালার্ট (হার্ডকোড) ----
    ENABLE_PRICE_ALERTS = True
    # ফ্রি, কোনো key লাগে না। Binance ব্যর্থ হলে (রিজিওন-ব্লক ইত্যাদি)
    # স্বয়ংক্রিয়ভাবে CoinGecko তে ফলব্যাক করা হয়।
    BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/price"
    COINGECKO_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
    # প্রতি ৩০-৪৫ সেকেন্ডে (এলোমেলো) দাম চেক করা হয়
    PRICE_CHECK_MIN_SECONDS = 30
    PRICE_CHECK_MAX_SECONDS = 45
    # মাইলস্টোন স্টেপ — ETH প্রতি $৫০, BTC প্রতি $৫০০
    ETH_MILESTONE_STEP = 50
    BTC_MILESTONE_STEP = 500


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
    # price_milestones টেবিলটি BTC/ETH এর সর্বশেষ ট্রিগার হওয়া মাইলস্টোন
    # সংরক্ষণ করে, যাতে একই মাইলস্টোনের আশেপাশে দাম ঘোরাঘুরি করলে
    # ডুপ্লিকেট অ্যালার্ট না যায় — শুধু নতুন মাইলস্টোন ক্রস করলেই একবার যায়।
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS price_milestones (
            symbol TEXT PRIMARY KEY,
            last_milestone REAL NOT NULL,
            last_price REAL,
            updated_at TEXT NOT NULL
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
# BTC/ETH প্রাইস মাইলস্টোন — SQLite এ সর্বশেষ ট্রিগার হওয়া মাইলস্টোন সেভ রাখা
# ---------------------------------------------------------------------------
def get_last_milestone(conn: sqlite3.Connection, symbol: str) -> Optional[float]:
    """এই সিম্বলের (BTCUSDT/ETHUSDT) জন্য সর্বশেষ ট্রিগার হওয়া মাইলস্টোন
    ডাটাবেস থেকে নিয়ে আসে। কখনো ট্র্যাক করা না হয়ে থাকলে None রিটার্ন করে —
    এই None-ই নির্দেশ করে যে এটাই প্রথমবার, তাই প্রথম চেকে কোনো অ্যালার্ট
    পাঠানো হয় না (শুধু বেসলাইন সেভ করা হয়)।"""
    cur = conn.execute(
        "SELECT last_milestone FROM price_milestones WHERE symbol=?", (symbol,)
    )
    row = cur.fetchone()
    return float(row[0]) if row is not None else None


def save_milestone(conn: sqlite3.Connection, symbol: str, milestone: float, price: float):
    """নতুন মাইলস্টোন ও সেই মুহূর্তের আসল দাম ডাটাবেসে সেভ/আপডেট করে।"""
    conn.execute(
        """
        INSERT INTO price_milestones (symbol, last_milestone, last_price, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            last_milestone=excluded.last_milestone,
            last_price=excluded.last_price,
            updated_at=excluded.updated_at
        """,
        (symbol, milestone, price, datetime.now(timezone.utc).isoformat()),
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


SEED_TEST_DELAY_SECONDS = 5  # বাগ #2 ফিক্স: প্রতিটি টেস্ট আইটেমের মাঝে বিরতি


def run_seed_test(conn: sqlite3.Connection):
    """৫টি টেস্ট নিউজ একে একে (sequentially) পুরো পাইপলাইনে
    (হেডলাইন -> ছবি -> পোস্ট) চালায়, যাতে ডেপ্লয় করার সাথে সাথেই বটের সব
    ফিচার নিজের চোখে যাচাই করা যায়।

    বাগ #2 ফিক্স:
      - সবগুলো (৫টি) আইটেম নিশ্চিতভাবে প্রসেস করা হয় — কোনো একটি আইটেমে
        সাময়িক (transient) এরর হলে সেটার জন্য একবার রিট্রাই করা হয়,
        তাতেও ব্যর্থ হলে লগ করে পরের আইটেমে চলে যাওয়া হয় — পুরো টেস্ট
        রান কখনোই মাঝপথে থেমে যায় না।
      - প্রতিটি আইটেমের পরে নিশ্চিতভাবে ৫ সেকেন্ড বিরতি দেওয়া হয় (হেডলাইন
        তৈরি ব্যর্থ হলেও), যাতে পরপর কলে রেট-লিমিটে না পড়ে।
    """
    seed_items = get_seed_news_items()
    total = len(seed_items)
    log.info("=" * 60)
    log.info(f"প্রথম রান শনাক্ত হয়েছে — {total}টি টেস্ট নিউজ পাঠানো শুরু হচ্ছে...")
    log.info("=" * 60)

    success_count = 0
    for idx, item in enumerate(seed_items, start=1):
        log.info(f"[টেস্ট {idx}/{total}] প্রসেস করা হচ্ছে: {item.title}")
        processed_ok = False
        for retry in range(2):  # প্রথম চেষ্টা + ১টি রিট্রাই
            try:
                process_item(conn, item)
                processed_ok = True
                break
            except Exception as e:
                if retry == 0:
                    log.warning(f"[টেস্ট {idx}/{total}] সাময়িক এরর হয়েছে, একবার রিট্রাই করা হচ্ছে: {e}")
                    time.sleep(3)
                else:
                    log.error(f"[টেস্ট {idx}/{total}] রিট্রাই করেও ব্যর্থ হয়েছে, পরের আইটেমে যাওয়া হচ্ছে: {e}")
        if processed_ok:
            success_count += 1

        if idx < total:
            log.info(f"[টেস্ট {idx}/{total}] সম্পন্ন — পরের টেস্ট আইটেমের আগে {SEED_TEST_DELAY_SECONDS} সেকেন্ড অপেক্ষা করা হচ্ছে।")
            time.sleep(SEED_TEST_DELAY_SECONDS)

    log.info(f"টেস্ট নিউজ পাঠানো সম্পন্ন হয়েছে ({success_count}/{total} সফলভাবে প্রসেস হয়েছে)। এখন থেকে বট স্বাভাবিক RSS/Twitter মোডে চলবে।")


# ---------------------------------------------------------------------------
# ফেচার — RSS নিউজ ও Nitter (X/Twitter) RSS
# ---------------------------------------------------------------------------
def clean_html(raw_html: str) -> str:
    """HTML ট্যাগ সরিয়ে শুধু টেক্সট বের করে আনে।"""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(separator=" ", strip=True)


def _normalize_image_url(url: str) -> str:
    """একটি ছবির URL কে তুলনা/ডুপ্লিকেট-চেকের জন্য নরমালাইজ করে —
    query string ও fragment বাদ দিয়ে, scheme/host লোয়ারকেস করে, এবং
    শেষের '/' বাদ দিয়ে। এতে একই ছবির ভিন্ন query-param (যেমন ?w=800 বনাম
    ?w=1200 বা ট্র্যাকিং প্যারামিটার) সহ URL গুলোকে একই ছবি হিসেবে ধরা যায়।"""
    try:
        parts = urllib.parse.urlsplit(url.strip())
        normalized_path = parts.path.rstrip("/")
        return urllib.parse.urlunsplit(
            (parts.scheme.lower(), parts.netloc.lower(), normalized_path, "", "")
        )
    except Exception:
        return (url or "").strip()


def dedupe_image_urls(urls: List[str]) -> List[str]:
    """URL লিস্ট থেকে নরমালাইজড-ডুপ্লিকেট বাদ দিয়ে প্রথমবার পাওয়া
    (আসল/original) URL গুলো ক্রমানুসারে রিটার্ন করে।"""
    seen = set()
    unique: List[str] = []
    for url in urls:
        if not url:
            continue
        key = _normalize_image_url(url)
        if key in seen:
            continue
        seen.add(key)
        unique.append(url)
    return unique


def entry_has_video(entry) -> bool:
    """একটি RSS/Nitter এন্ট্রিতে ভিডিও, GIF, বা ভিডিও-থাম্বনেইল আছে কিনা
    শনাক্ত করে (media:content এর medium/type, enclosure লিংকের type, অথবা
    HTML বডির ভেতরে <video> ট্যাগ/.mp4 লিংক দেখে)। এই ফাংশনটি বাগ ফিক্স
    হিসেবে যোগ করা হয়েছে — X/Twitter পোস্টে ভিডিও থাকলে সেটা কখনোই সরাসরি
    ডাউনলোড/পাঠানো হয় না; বরং ছবি-বিহীন ধরে Pollinations.ai দিয়ে AI ছবি
    জেনারেট করে পাঠানো হয় (fetch_rss_news/fetch_twitter_news দ্রষ্টব্য)।"""
    for m in getattr(entry, "media_content", []) or []:
        medium = str(m.get("medium", "")).lower()
        mtype = str(m.get("type", "")).lower()
        if medium == "video" or mtype.startswith("video") or "gif" in mtype:
            return True

    for link in getattr(entry, "links", []) or []:
        ltype = str(link.get("type", "")).lower()
        if ltype.startswith("video") or "gif" in ltype:
            return True

    html_blob = ""
    if hasattr(entry, "summary"):
        html_blob += entry.summary or ""
    for c in getattr(entry, "content", []) or []:
        html_blob += c.get("value", "") or ""
    if html_blob:
        lowered = html_blob.lower()
        if "<video" in lowered or ".mp4" in lowered or "video_thumb" in lowered:
            return True

    return False


def extract_images_from_entry(entry) -> List[str]:
    """RSS এন্ট্রি থেকে সর্বোচ্চ ২টি ছবির URL বের করার চেষ্টা করে
    (media:content, media:thumbnail, enclosure, অথবা HTML এর ভেতরের <img>)।
    সংগ্রহ করা সব URL শেষে normalized-dedupe করা হয়, যাতে একই ছবি
    ভিন্ন query-param সহ দুইবার এলেও সেটা ডুপ্লিকেট হিসেবে বাদ যায়।
    এই এন্ট্রিতে ভিডিও/GIF থাকলে (entry_has_video) কোনো ছবিই রিটার্ন করা
    হয় না — সেক্ষেত্রে fetch_rss_news/fetch_twitter_news পরে
    Pollinations.ai দিয়ে AI ছবি জেনারেট করবে।"""
    if entry_has_video(entry):
        return []

    images: List[str] = []

    for m in getattr(entry, "media_content", []) or []:
        url = m.get("url")
        if url:
            images.append(url)

    for m in getattr(entry, "media_thumbnail", []) or []:
        url = m.get("url")
        if url:
            images.append(url)

    for link in getattr(entry, "links", []) or []:
        if link.get("rel") == "enclosure" and str(link.get("type", "")).startswith("image"):
            url = link.get("href")
            if url:
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
            if src:
                images.append(src)

    return dedupe_image_urls(images)[:2]


def parse_feed_safely(feed_url: str, timeout: int = None):
    """RSS/Nitter ফিড টাইমআউটসহ ফেচ করে feedparser দিয়ে পার্স করে।
    সরাসরি feedparser.parse(url) ব্যবহার করলে কোনো সার্ভার সাড়া না দিলে
    (বিশেষত অস্থির পাবলিক Nitter ইনস্ট্যান্স) রিকোয়েস্টটি অনির্দিষ্টকাল আটকে
    থাকতে পারে — requests.get(timeout=...) ব্যবহার করে সেটা এড়ানো হয়েছে,
    যাতে কোনো একটা সোর্স সাড়া না দিলেও পুরো সাইকেল আটকে না থেকে দ্রুত
    পরের সোর্সে চলে যায়। ব্যর্থ হলে exception raise করে — কলার সেটা catch
    করে লগ করে পরের সোর্সে এগিয়ে যায়।"""
    resp = requests.get(
        feed_url,
        timeout=timeout or Config.FEED_FETCH_TIMEOUT_SECONDS,
        headers={"User-Agent": "Mozilla/5.0 (compatible; CryptoBartaBot/1.0)"},
    )
    resp.raise_for_status()
    return feedparser.parse(resp.content)


def fetch_rss_news() -> List[NewsItem]:
    """প্রাইমারি ও সবচেয়ে নির্ভরযোগ্য সোর্স — Cointelegraph, CoinDesk ও
    Decrypt এর অফিসিয়াল RSS ফিড থেকে সর্বশেষ খবরগুলো সংগ্রহ করে। কোনো একটা
    ফিড সাময়িকভাবে অকেজো থাকলে (টাইমআউট/এরর) সেটা লগ করে বাকি ফিডগুলো
    থেকে সংগ্রহ চালিয়ে যায় — একটার ব্যর্থতায় পুরো সাইকেল থামে না।"""
    items: List[NewsItem] = []
    for feed_url in Config.RSS_FEEDS:
        try:
            parsed = parse_feed_safely(feed_url)
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
    """সেকেন্ডারি/বেস্ট-এফোর্ট সোর্স — পাবলিক Nitter RSS ইনস্ট্যান্স থেকে
    টুইট সংগ্রহ করে (সম্পূর্ণ ফ্রি, কোনো Twitter API key লাগে না)। প্রতিটি
    অ্যাকাউন্টের ফিড আলাদাভাবে টাইমআউটসহ ফেচ করা হয় (parse_feed_safely) —
    কোনো একাউন্টের ফিড সাময়িকভাবে না পাওয়া গেলে (Nitter ইনস্ট্যান্স ডাউন,
    টাইমআউট ইত্যাদি) সেটা নিঃশব্দে/গ্রেসফুলি স্কিপ হয়ে পরের অ্যাকাউন্টে
    চলে যায় — পুরো সাইকেল কখনো আটকে থাকে না বা ব্যর্থ হয় না।

    কোনো পোস্টে ভিডিও/GIF থাকলে extract_images_from_entry()
    স্বয়ংক্রিয়ভাবে সেটার ছবি ফাঁকা রাখে, ফলে post_news_item() সেটাকে
    ছবি-বিহীন আইটেম হিসেবে ধরে Pollinations.ai দিয়ে AI ছবি বানিয়ে পাঠায় —
    ভিডিও কখনো ডাউনলোড/পাঠানো হয় না।

    নোট: পাবলিক Nitter ইনস্ট্যান্সগুলো মাঝে মাঝে ডাউন থাকে। কাজ না করলে
    NITTER_BASE পরিবর্তন করুন অথবা ENABLE_TWITTER=false সেট করে শুধু RSS দিয়ে চালান।
    """
    if not Config.ENABLE_TWITTER:
        return []
    items: List[NewsItem] = []
    for username in Config.TWITTER_USERNAMES:
        feed_url = f"{Config.NITTER_BASE.rstrip('/')}/{username}/rss"
        try:
            parsed = parse_feed_safely(feed_url)
            if not parsed.entries:
                log.warning(f"@{username} এর জন্য Nitter থেকে কিছু পাওয়া যায়নি (ইনস্ট্যান্স ডাউন থাকতে পারে) — স্কিপ করা হচ্ছে")
                continue
            for entry in parsed.entries[:5]:
                link = entry.get("link")
                if not link:
                    continue
                title = clean_html(entry.get("title", ""))
                summary = clean_html(entry.get("description", ""))
                if entry_has_video(entry):
                    log.info(f"@{username} এর একটি পোস্টে ভিডিও/GIF পাওয়া গেছে — ভিডিও স্কিপ করে AI ছবি ব্যবহার হবে")
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
            log.warning(f"@{username} এর Nitter ফিড ফেচ করতে ব্যর্থ (গ্রেসফুলি স্কিপ করা হচ্ছে, সাইকেল থামছে না): {e}")
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


def ai_rate_limit_sleep():
    """বাগ #1 ফিক্স: প্রতিটি Gemini API কলের পর ৩-৫ সেকেন্ড এলোমেলো বিরতি
    দেয়, যাতে বার্স্ট-লিমিটে ধাক্কা না লাগে।"""
    delay = random.uniform(Config.AI_CALL_MIN_DELAY, Config.AI_CALL_MAX_DELAY)
    time.sleep(delay)


def _is_rate_limit_error(exc: Exception) -> bool:
    """এরর মেসেজে 429 / RESOURCE_EXHAUSTED / quota থাকলে সেটাকে
    রেট-লিমিট/কোটা এরর হিসেবে শনাক্ত করে।"""
    text = str(exc).lower()
    return "429" in text or "resource_exhausted" in text or "quota" in text


def _call_gemini(prompt: str):
    """Gemini কে কল করে — প্রথমে প্রাইমারি মডেল দিয়ে, রেট-লিমিট/কোটা এরর
    পেলে ফলব্যাক মডেল দিয়ে আবার চেষ্টা করে। প্রতিটি কলের পরেই
    ai_rate_limit_sleep() দিয়ে বিরতি দেওয়া হয় (সফল হোক বা ব্যর্থ)।"""
    client = get_genai_client()
    models_to_try = [Config.GEMINI_TEXT_MODEL, Config.GEMINI_FALLBACK_MODEL]
    last_exc = None
    for model_name in models_to_try:
        try:
            resp = client.models.generate_content(model=model_name, contents=prompt)
            return resp
        except Exception as e:
            last_exc = e
            if _is_rate_limit_error(e):
                log.warning(f"মডেল '{model_name}' কোটা/রেট-লিমিটে ব্যর্থ, ফলব্যাক মডেল চেষ্টা করা হচ্ছে: {e}")
                continue
            raise
        finally:
            ai_rate_limit_sleep()
    raise last_exc


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

2. "headline": a single Bengali (বাংলা) sentence that is a CORE INCIDENT
   SUMMARY of the news — not a generic teaser.

STRICT RULES FOR "headline" (বাগ #3 ফিক্স — কোর ইনসিডেন্ট সামারি):
- It MUST state the specific WHAT HAPPENED: the concrete event, action,
  number, amount, percentage, entity name, or outcome taken directly from
  the "Title"/"Details" below. Name who did what, to whom/what, and any
  figures involved, exactly like a news-agency one-line summary.
  Example of the CORRECT style: "নর্থ কোরিয়ার হ্যাকারদের হামলায় বিটগেটের
  ৩৫২ মিলিয়ন ডলার ক্ষতি হয়েছে বলে সন্দেহ করা হচ্ছে।"
- It is STRICTLY FORBIDDEN to write generic teaser/filler phrases such as
  "আজকের সেরা খবর", "বিস্তারিত জেনে নিন", "দামের ওঠানামা", "জেনে নিন",
  "সর্বশেষ খবর", or any sentence that only says news exists without stating
  the actual fact. If you catch yourself writing a teaser, rewrite it as a
  factual summary instead.
- Bengali text ONLY. No quotes, no English words, no hashtags, and NO emoji
  inside this field (the emoji goes only in the separate "emoji" field above).
- Length MUST be between {min_c} and {max_c} characters (including spaces).
- Base it strictly on the facts given below — never invent numbers or facts
  that are not present in the content. If the content has no specific number,
  summarize the specific action/decision/entity instead.

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


# বাগ #3 ফিক্স: এই জেনেরিক টিজার বাক্যাংশগুলো হেডলাইনে থাকলে সেটাকে
# ব্যর্থ ধরে নিয়ে Gemini কে আবার লিখতে বলা হয় — কারণ এগুলো "কোর ইনসিডেন্ট"
# বলে না, শুধু "খবর আছে" বলে।
TEASER_PHRASES_BN = [
    "সেরা খবর",
    "বিস্তারিত জেনে নিন",
    "জেনে নিন",
    "সর্বশেষ খবর",
    "দামের ওঠানামা",
    "আজকের খবর",
]


def _headline_has_teaser_phrase(headline: str) -> bool:
    return any(phrase in headline for phrase in TEASER_PHRASES_BN)


def generate_bengali_headline(item: NewsItem) -> Optional[Tuple[str, str]]:
    """Gemini দিয়ে ১০০-১৫০ অক্ষরের বাংলা "কোর ইনসিডেন্ট সামারি" হেডলাইন এবং
    প্রাসঙ্গিক ইমোজি তৈরি করে। (emoji, headline) টাপল রিটার্ন করে, অথবা
    ব্যর্থ হলে None। অক্ষরসংখ্যা সীমার মধ্যে না এলে বা জেনেরিক টিজার বাক্য
    থাকলে সর্বোচ্চ ৩ বার চেষ্টা করে। প্রতিটি Gemini কল রেট-লিমিট এড়াতে
    প্রাইমারি/ফলব্যাক মডেল ও ৩-৫ সেকেন্ড বিরতি ব্যবহার করে (_call_gemini)।"""
    prompt = HEADLINE_PROMPT_TEMPLATE.format(
        min_c=Config.HEADLINE_MIN_CHARS,
        max_c=Config.HEADLINE_MAX_CHARS,
        title=item.title,
        summary=item.summary[:500],
    )
    for attempt in range(3):
        try:
            resp = _call_gemini(prompt)
            data = _parse_headline_json(resp.text or "")
            if not data:
                log.warning(f"JSON পার্স করা যায়নি, আবার চেষ্টা করা হচ্ছে ({attempt + 1}/3)")
                continue

            headline = str(data.get("headline", "")).strip().strip('"').strip()
            headline = re.sub(r"\s+", " ", headline)
            emoji = _sanitize_emoji(str(data.get("emoji", "")))
            length = len(headline)

            if _headline_has_teaser_phrase(headline):
                log.info(f"টিজার/জেনেরিক বাক্য ধরা পড়েছে, আবার চেষ্টা করা হচ্ছে ({attempt + 1}/3): {headline}")
                prompt += (
                    "\n\nYour previous attempt used a generic teaser phrase instead of "
                    "stating the core incident/fact. Rewrite it as a specific factual "
                    "summary of what actually happened, with names/numbers from the "
                    "content. Respond with the same JSON shape."
                )
                continue

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
        resp = _call_gemini(prompt)
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
    ২টি ভিন্ন সোর্স ছবি -> অ্যালবাম | ১টি (বা ডুপ্লিকেট) সোর্স ছবি -> একক ছবি |
    কোনো ছবি নেই -> Pollinations.ai দিয়ে ছবি বানিয়ে পাঠানো | সব ব্যর্থ হলে -> শুধু টেক্সট।

    ডুপ্লিকেট-ইমেজ বাগ ফিক্স: পোস্ট করার ঠিক আগে আবার dedupe_image_urls()
    দিয়ে normalized-dedupe করা হয়, যাতে একই ছবি দুইবার (query-param ভিন্ন
    হলেও) অ্যালবাম হিসেবে না যায়। সত্যিকারের ২টি আলাদা ইউনিক ছবি থাকলে
    তবেই sendMediaGroup ব্যবহার হয়, নাহলে sendPhoto দিয়ে একটাই পাঠানো হয়।"""
    caption = build_caption(emoji, headline_bn, item.link)
    raw_images = [u for u in item.images if u and u.startswith("http")]
    valid_images = dedupe_image_urls(raw_images)

    if len(valid_images) >= 2:
        log.info("২টি ভিন্ন (ইউনিক) সোর্স ছবি দিয়ে অ্যালবাম আকারে পোস্ট করা হচ্ছে")
        return send_media_group(caption, valid_images[:2])

    if len(valid_images) == 1:
        log.info("১টি ইউনিক সোর্স ছবি দিয়ে পোস্ট করা হচ্ছে (ডুপ্লিকেট থাকলে বাদ দেওয়া হয়েছে)")
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
# BTC/ETH রিয়েল-টাইম প্রাইস মাইলস্টোন অ্যালার্ট (নতুন ফিচার)
# ---------------------------------------------------------------------------
# Pollinations.ai এর জন্য ইমেজ প্রম্পট — প্রতিটি মাইলস্টোন অ্যালার্টে এই
# একই প্রম্পট ব্যবহার হয় (৩D আর্ট, কোনো টেক্সট/অক্ষর/ওয়াটারমার্ক ছাড়া)।
ETH_PRICE_IMAGE_PROMPT = (
    "glowing futuristic neon crystal Ethereum gem floating in dark space, "
    "energetic light beams, clean background, 3D render, 4k, no text, "
    "no letters, no words"
)
BTC_PRICE_IMAGE_PROMPT = (
    "luxurious glowing golden Bitcoin coin rising in dark digital space, "
    "energetic light beams, cinematic lighting, 3D render, 4k, no text, "
    "no letters, no words"
)

PRICE_SYMBOLS = {
    # symbol (Binance) -> (লেবেল, milestone step, image prompt, ক্যাপশনের handle)
    "ETHUSDT": ("Ethereum", Config.ETH_MILESTONE_STEP, ETH_PRICE_IMAGE_PROMPT, "@ETH_PRICE"),
    "BTCUSDT": ("Bitcoin", Config.BTC_MILESTONE_STEP, BTC_PRICE_IMAGE_PROMPT, "@BTC_PRICE"),
}

COINGECKO_IDS = {"BTCUSDT": "bitcoin", "ETHUSDT": "ethereum"}


def fetch_spot_price(symbol: str) -> Optional[float]:
    """সম্পূর্ণ ফ্রি পাবলিক API দিয়ে লাইভ স্পট প্রাইস আনে — প্রথমে Binance,
    সেটা ব্যর্থ হলে (রিজিওন-ব্লক/ডাউনটাইম) স্বয়ংক্রিয়ভাবে CoinGecko তে
    ফলব্যাক করে। কোনো API key প্রয়োজন হয় না।"""
    try:
        resp = requests.get(
            Config.BINANCE_TICKER_URL, params={"symbol": symbol}, timeout=10
        )
        resp.raise_for_status()
        return float(resp.json()["price"])
    except Exception as e:
        log.warning(f"Binance থেকে {symbol} এর দাম আনতে ব্যর্থ, CoinGecko চেষ্টা করা হচ্ছে: {e}")

    try:
        coin_id = COINGECKO_IDS.get(symbol)
        if not coin_id:
            return None
        resp = requests.get(
            Config.COINGECKO_PRICE_URL,
            params={"ids": coin_id, "vs_currencies": "usd"},
            timeout=10,
        )
        resp.raise_for_status()
        return float(resp.json()[coin_id]["usd"])
    except Exception as e:
        log.error(f"CoinGecko থেকেও {symbol} এর দাম আনতে ব্যর্থ: {e}")
        return None


def check_price_milestone(conn: sqlite3.Connection, symbol: str):
    """একটি সিম্বলের (BTCUSDT/ETHUSDT) বর্তমান দাম চেক করে সংশ্লিষ্ট
    মাইলস্টোন স্টেপে রাউন্ড করে। আগের সেভ করা মাইলস্টোনের সাথে তুলনা করে —
    সেটা বদলে গেলে (এবং এটাই প্রথমবার না হলে) দিক (📈/📉) নির্ণয় করে একটি
    Pollinations.ai আর্ট-সহ একলাইনের অ্যালার্ট পোস্ট করে। একই মাইলস্টোনের
    আশেপাশে দাম ঘোরাঘুরি করলে (মাইলস্টোন অপরিবর্তিত থাকলে) কিছুই পাঠানো
    হয় না — এভাবে ডুপ্লিকেট অ্যালার্ট আটকানো হয়।"""
    label, step, image_prompt, handle = PRICE_SYMBOLS[symbol]

    price = fetch_spot_price(symbol)
    if price is None:
        log.warning(f"{label} এর দাম পাওয়া যায়নি, এই চেকটি স্কিপ করা হচ্ছে।")
        return

    milestone = math.floor(price / step) * step
    last_milestone = get_last_milestone(conn, symbol)

    if last_milestone is None:
        # প্রথমবার ট্র্যাক করা হচ্ছে — শুধু বেসলাইন সেভ করা হয়, কোনো
        # অ্যালার্ট পাঠানো হয় না (নাহলে বট স্টার্ট হওয়ামাত্র একটা ভুয়া
        # অ্যালার্ট চলে যাবে)।
        save_milestone(conn, symbol, milestone, price)
        log.info(f"{label} প্রাইস ট্র্যাকিং শুরু — বেসলাইন মাইলস্টোন ${milestone:,.0f} সেভ করা হলো।")
        return

    if milestone == last_milestone:
        return  # একই মাইলস্টোনের ভেতরে ঘোরাঘুরি করছে — অ্যালার্ট নেই

    direction_emoji = "📈" if milestone > last_milestone else "📉"
    log.info(f"{label} মাইলস্টোন ক্রস হয়েছে: ${last_milestone:,.0f} -> ${milestone:,.0f} ({direction_emoji})")
    save_milestone(conn, symbol, milestone, price)

    formatted_price = f"{milestone:,.0f}"
    caption = (
        f'{direction_emoji} <b>${formatted_price}</b> '
        f'<a href="{Config.FOLLOW_CHANNEL_URL}"><b>{handle}</b></a>'
    )

    image_bytes = generate_ai_image_bytes(image_prompt)
    if image_bytes:
        sent = send_single_photo(caption, image_bytes=image_bytes)
    else:
        # Pollinations.ai ছবি বানাতে ব্যর্থ হলেও, দামের অ্যালার্মটা যেন
        # মিস না হয়ে যায় তাই টেক্সট আকারেই পাঠানো হয়।
        log.warning(f"{label} এর জন্য Pollinations.ai ছবি তৈরিতে ব্যর্থ, শুধু টেক্সট পাঠানো হচ্ছে।")
        sent = send_text_message(caption)

    if sent:
        log.info(f"{label} মাইলস্টোন অ্যালার্ট পোস্ট হয়েছে: {direction_emoji} ${formatted_price}")


def price_alert_loop():
    """ব্যাকগ্রাউন্ড থ্রেডে অনন্তকাল ধরে চলতে থাকে — প্রতি ৩০-৪৫ সেকেন্ডে
    (এলোমেলো) BTC ও ETH এর দাম চেক করে। এটি নিউজ পাইপলাইনের main() লুপ
    থেকে সম্পূর্ণ স্বতন্ত্র/non-blocking, তাই দুটো একসাথে স্বাভাবিকভাবে
    চলতে থাকে। এই থ্রেডের জন্য নিজস্ব SQLite কানেকশন ব্যবহার করা হয়
    (sqlite3 কানেকশন থ্রেড-সেফ নয়, তাই মূল লুপের কানেকশনের সাথে শেয়ার
    করা হয় না)।"""
    conn = get_db()
    log.info("BTC/ETH প্রাইস মাইলস্টোন অ্যালার্ট ব্যাকগ্রাউন্ড থ্রেড চালু হয়েছে।")
    while True:
        for symbol in PRICE_SYMBOLS:
            try:
                check_price_milestone(conn, symbol)
            except Exception as e:
                log.error(f"{symbol} প্রাইস চেক করতে সমস্যা হয়েছে: {e}")
        sleep_time = random.uniform(
            Config.PRICE_CHECK_MIN_SECONDS, Config.PRICE_CHECK_MAX_SECONDS
        )
        time.sleep(sleep_time)


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
    """মূল ফাংশন — কনফিগারেশন যাচাই করে সরাসরি আসল RSS/Twitter মনিটরিং লুপ
    চালু করে। (সিড/টেস্ট নিউজ সম্পূর্ণভাবে বন্ধ করা হয়েছে — নিচের নোট দেখুন।)"""
    validate_config()
    conn = get_db()
    log.info("Crypto Barta বট চালু হয়েছে।")

    # ৫টি মক/সিড টেস্ট নিউজ পাঠানোর ফিচারটি সম্পূর্ণভাবে বন্ধ করা হয়েছে —
    # বট স্টার্টআপেই কোনো কৃত্রিম/টেস্ট নিউজ পাঠাবে না, প্রথম সাইকেল থেকেই
    # শুধুমাত্র আসল RSS ফিড ও Twitter সোর্স থেকে লাইভ নিউজ মনিটর করবে।
    # (run_seed_test() ও Config.ENABLE_SEED_TEST ফাংশন/সেটিং কোডে থেকে গেলেও
    # এখান থেকে আর কল করা হয় না, তাই কখনো চলবে না।)

    # BTC/ETH প্রাইস মাইলস্টোন অ্যালার্ট একটি আলাদা non-blocking ব্যাকগ্রাউন্ড
    # থ্রেডে চালু করা হয় (daemon=True রাখা হয়েছে যাতে মূল প্রোগ্রাম বন্ধ হলে
    # থ্রেডও নিজে থেকে বন্ধ হয়ে যায়) — এটি নিউজ সাইকেলের সাথে সমান্তরালে চলে।
    if Config.ENABLE_PRICE_ALERTS:
        threading.Thread(target=price_alert_loop, daemon=True).start()
    else:
        log.info("ENABLE_PRICE_ALERTS=false — প্রাইস মাইলস্টোন অ্যালার্ট বন্ধ রাখা হয়েছে।")

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
