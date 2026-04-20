import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# =========================
# LOAD ENVIRONMENT VARIABLES
# =========================
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

print("=" * 60)
print("📡 CONNECTING TO MONGODB ATLAS...")
print("=" * 60)

try:
    client = MongoClient(MONGO_URI)
    client.admin.command('ping')
    print("✅ CONNECTION SUCCESS!\n")
except Exception as e:
    print(f"❌ CONNECTION FAILED: {e}\n")
    exit()

db = client[DB_NAME]
collection = db[COLLECTION_NAME]

print(f"📊 Database: {DB_NAME}")
print(f"📋 Collection: {COLLECTION_NAME}\n")

headers = {
    "User-Agent": "Mozilla/5.0"
}

BASE_URL = "https://www.cnbcindonesia.com/news"


# =========================
# AMBIL LINK ARTIKEL
# =========================
def get_article_links():
    links = []

    try:
        print("🔍 Mengambil link artikel...")
        res = requests.get(BASE_URL, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")

        articles = soup.find_all("a", href=True)

        for a in articles:
            link = a["href"]

            if "/news/" in link:
                if link.startswith("/"):
                    link = "https://www.cnbcindonesia.com" + link

                links.append(link)

    except Exception as e:
        print("Error ambil link:", e)

    unique_links = list(set(links))
    print(f"✅ Total {len(unique_links)} link ditemukan\n")
    return unique_links


# =========================
# AMBIL DETAIL ARTIKEL
# =========================
def get_article_detail(url):
    try:
        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")

        # Judul
        title = soup.find("h1")
        title = title.text.strip() if title else None

        # Tanggal
        date = soup.find("meta", property="article:published_time")
        date = date["content"] if date else None

        # Author
        author = soup.find("meta", {"name": "author"})
        author = author["content"] if author else None

        # Thumbnail
        thumbnail = soup.find("meta", property="og:image")
        thumbnail = thumbnail["content"] if thumbnail else None

        # Isi berita
        content_div = soup.find("div", class_="detail_text")
        content = content_div.get_text(strip=True) if content_div else None

        # Tag kategori
        tags = []
        tag_div = soup.find_all("a")
        for t in tag_div:
            if "/tag/" in t.get("href", ""):
                tags.append(t.text.strip())

        # FILTER keyword environment
        keyword = ["lingkungan", "environment", "sustainability", "emisi", "climate"]
        if content:
            if not any(k in content.lower() for k in keyword):
                return None

        data = {
            "url": url,
            "judul": title,
            "tanggal": date,
            "author": author,
            "tag": tags,
            "isi": content,
            "thumbnail": thumbnail,
            "created_at": datetime.now()
        }

        return data

    except Exception as e:
        return None


# =========================
# MAIN PROGRAM
# =========================
def main():
    links = get_article_links()
    print(f"Total link: {len(links)}\n")

    saved = 0
    skipped = 0
    failed = 0

    for idx, link in enumerate(links, 1):
        print(f"[{idx}/{len(links)}] Processing: {link[:50]}...")

        # Cek duplikat
        if collection.find_one({"url": link}):
            print(f"    ⏭️  Sudah ada di database, skip\n")
            skipped += 1
            continue

        data = get_article_detail(link)

        if data:
            result = collection.insert_one(data)
            print(f"    ✅ TERSIMPAN: {data['judul'][:50]}")
            print(f"    ID: {result.inserted_id}\n")
            saved += 1
        else:
            print(f"    ✖️  Tidak sesuai keyword\n")
            failed += 1

        time.sleep(2)

    print("=" * 60)
    print("🎉 SELESAI!")
    print(f"✅ Tersimpan: {saved}")
    print(f"⏭️  Skip: {skipped}")
    print(f"✖️  Gagal: {failed}")
    print(f"📊 Database: {DB_NAME}/{COLLECTION_NAME}")
    print("=" * 60)


if __name__ == "__main__":
    main()