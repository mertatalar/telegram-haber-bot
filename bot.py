import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

import feedparser
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ---------- LOG ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("haber-bot")

# ---------- .env ----------
load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID_ENV = os.getenv("CHAT_ID", "").strip()

# Örnek: FEED_URLS=https://www.cnbce.com/rss
FEED_URLS = [u.strip() for u in os.getenv("FEED_URLS", "").split(",") if u.strip()]

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))

HTML_SCRAPE_ENABLED = os.getenv("HTML_SCRAPE_ENABLED", "false").lower() == "true"
HTML_SCRAPE_URL = os.getenv("HTML_SCRAPE_URL", "").strip()
HTML_ITEM_SELECTOR = os.getenv("HTML_ITEM_SELECTOR", "").strip()
HTML_TITLE_SELECTOR = os.getenv("HTML_TITLE_SELECTOR", "").strip()
HTML_LINK_SELECTOR = os.getenv("HTML_LINK_SELECTOR", "").strip()
HTML_SUMMARY_SELECTOR = os.getenv("HTML_SUMMARY_SELECTOR", "").strip()
HTML_PUBLISHED_SELECTOR = os.getenv("HTML_PUBLISHED_SELECTOR", "").strip()

ALLOWED_SETCHAT = {
    int(x) for x in re.findall(r"\d+", os.getenv("ALLOWED_SETCHAT_USER_IDS", ""))
} if os.getenv("ALLOWED_SETCHAT_USER_IDS") else None

DB_PATH = "data.db"


# ---------- MODEL ----------
@dataclass
class NewsItem:
    id: str
    title: str
    link: str
    summary: Optional[str] = None
    published: Optional[datetime] = None
    image_url: Optional[str] = None  # şu an kullanmıyoruz ama dursun


# ---------- SQLITE STORE ----------
class Store:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS posted (id TEXT PRIMARY KEY, created_at TEXT)"
        )
        self.conn.commit()

    def get(self, key: str) -> Optional[str]:
        cur = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

    def set(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def mark_posted(self, item_id: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO posted(id, created_at) VALUES(?, ?)",
            (item_id, datetime.now(timezone.utc).isoformat()),
        )
        self.conn.commit()

    def is_posted(self, item_id: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM posted WHERE id = ?", (item_id,))
        return cur.fetchone() is not None


store = Store(DB_PATH)


# ---------- RSS OKUMA ----------
def fetch_rss(url: str) -> List[NewsItem]:
    """
    RSS feed'den haberleri çek.
    ÖZEL: Sadece linkinde 'piyasalar' geçen haberleri al.
    CNBC için FEED_URLS=https://www.cnbce.com/rss kullanabilirsin.
    """
    parsed = feedparser.parse(url)
    items: List[NewsItem] = []

    for e in parsed.entries:
        link = str(getattr(e, "link", ""))

        # Sadece piyasalar haberleri
        if "piyasalar" not in link.lower():
            continue

        item_id = (
            getattr(e, "id", None)
            or getattr(e, "guid", None)
            or link
            or getattr(e, "title", "")
        )
        if not item_id:
            continue

        published = None
        if getattr(e, "published_parsed", None):
            published = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)

        raw_summary = getattr(e, "summary", "") if getattr(e, "summary", None) else ""
        summary_text = None

        if raw_summary:
            soup = BeautifulSoup(raw_summary, "html.parser")
            summary_text = soup.get_text(" ", strip=True)

        items.append(
            NewsItem(
                id=str(item_id),
                title=str(getattr(e, "title", "(Başlık yok)")),
                link=link,
                summary=summary_text,
                published=published,
                image_url=None,
            )
        )

    return items


# ---------- HTML SCRAPING (OPSİYONEL, ŞU AN KULLANMIYORUZ) ----------
def fetch_html() -> List[NewsItem]:
    if not (HTML_SCRAPE_URL and HTML_ITEM_SELECTOR and HTML_TITLE_SELECTOR and HTML_LINK_SELECTOR):
        return []

    r = requests.get(HTML_SCRAPE_URL, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    items: List[NewsItem] = []

    for box in soup.select(HTML_ITEM_SELECTOR):
        title_el = box.select_one(HTML_TITLE_SELECTOR)
        link_el = box.select_one(HTML_LINK_SELECTOR)
        if not (title_el and link_el):
            continue

        title = title_el.get_text(strip=True)
        link = link_el.get("href") or ""
        if link and link.startswith("/"):
            from urllib.parse import urljoin
            link = urljoin(HTML_SCRAPE_URL, link)

        summary = None
        if HTML_SUMMARY_SELECTOR:
            s_el = box.select_one(HTML_SUMMARY_SELECTOR)
            summary = s_el.get_text(strip=True) if s_el else None

        published = None
        if HTML_PUBLISHED_SELECTOR:
            p_el = box.select_one(HTML_PUBLISHED_SELECTOR)
            if p_el and p_el.has_attr("datetime"):
                try:
                    published = datetime.fromisoformat(p_el["datetime"]).astimezone(
                        timezone.utc
                    )
                except Exception:
                    published = None

        item_id = link or title
        items.append(
            NewsItem(
                id=item_id,
                title=title,
                link=link,
                summary=summary,
                published=published,
                image_url=None,
            )
        )

    return items


# ----------- HABER GÖNDERME (BAŞLIK + ÖZET + ETİKETLER) -----------
async def send_item(context: ContextTypes.DEFAULT_TYPE, chat_id: int, item: NewsItem) -> None:
    """
    Tek mesaj:
    - Başlık
    - Özet
    - Etiketler
    """

    parts = [item.title]

    if item.summary:
        parts.append("")
        parts.append(item.summary)

    # Etiketler
    parts.append("")
    parts.append("#Borsa #Ekonomi #Bist100 #Anlıkhaber")

    text = "\n".join(parts).strip()

    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
    )


# ---------- KOMUTLAR ----------
async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("pong ✅")


async def setchat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat:
        return

    user_id = update.effective_user.id if update.effective_user else None
    if ALLOWED_SETCHAT is not None and user_id not in ALLOWED_SETCHAT:
        await update.message.reply_text("Bu komutu kullanma yetkiniz yok.")
        return

    chat_id = update.effective_chat.id
    store.set("CHAT_ID", str(chat_id))
    await update.message.reply_text(f"Hedef chat kaydedildi: {chat_id}")


# ---------- JOB: OTOMATİK HABER ÇEK ----------
async def job_poll_news(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    - RSS kaynaklarını okur
    - Daha önce gönderilmemiş yeni haberlerden EN YENİ 3 TANESİNİ seçer
    - Bu 3 haberi 30 saniye aralıklarla gönderir
    """
    try:
        chat_id_value = store.get("CHAT_ID") or CHAT_ID_ENV
        if not chat_id_value:
            logger.warning("CHAT_ID ayarlı değil; /setchat komutunu hedef grupta çalıştırın.")
            return

        chat_id = int(chat_id_value)
        new_items: List[NewsItem] = []

        # RSS kaynaklarından haberleri al
        for url in FEED_URLS:
            try:
                new_items.extend(fetch_rss(url))
            except Exception as e:
                logger.exception("RSS okuma hatası (%s): %s", url, e)

        # HTML scraping açıksa ekle
        if HTML_SCRAPE_ENABLED:
            try:
                new_items.extend(fetch_html())
            except Exception as e:
                logger.exception("HTML scraping hatası: %s", e)

        # Daha önce gönderilmemiş olanlar
        to_post = [i for i in new_items if not store.is_posted(i.id)]

        if not to_post:
            return

        # Eski → yeni sıralama
        def sort_key(i: NewsItem):
            return i.published or datetime.now(timezone.utc)

        to_post.sort(key=sort_key)

        # SON 3 HABERİ AL
        latest_three = to_post[-3:]

        # HER HABERİ 30 SANİYE ARAYLA GÖNDER
        for item in latest_three:
            try:
                await send_item(context, chat_id, item)
                store.mark_posted(item.id)
            except Exception:
                logger.exception("Gönderim hatası")

            # Son haber değilse 30 saniye bekle
            if item != latest_three[-1]:
                await asyncio.sleep(30)

    except Exception:
        logger.exception("Job içinde beklenmeyen hata")



# ---------- /fetch (ELLE TETİKLEME) ----------
async def fetch_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Haber kaynakları kontrol ediliyor...")
    await job_poll_news(context)
    await update.message.reply_text("Kontrol bitti ✅ (yeni haber varsa yukarıda gözükecek)")


# ---------- MAIN ----------
def main():
    if not TOKEN:
        raise SystemExit("TELEGRAM_BOT_TOKEN tanımlı değil (.env dosyasını kontrol et).")

    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("ping", ping))
    application.add_handler(CommandHandler("setchat", setchat))
    application.add_handler(CommandHandler("fetch", fetch_now))

    application.job_queue.run_repeating(
        job_poll_news,
        interval=CHECK_INTERVAL,
        first=0,
    )

    logger.info("Bot çalışıyor...")
    application.run_polling()


if __name__ == "__main__":
    main()
