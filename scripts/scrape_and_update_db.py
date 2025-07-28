from pymongo import MongoClient # YENİ: En üste ekleyin
from pymongo.database import Database # YENİ: En üste ekleyin
import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed  # GÜNCELLEME: Paralel işlem için eklendi

# --- PROJE DİZİNİNİ OTOMATİK BULMA ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- AYARLAR ---
# DEĞİŞTİ: SQLite yerine MongoDB ayarları
# BU BİLGİLERİ GITHUB ACTIONS SECRETS'TEN ALACAĞIZ!
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = "GamesDB" # Veritabanı adımız

INPUT_CSV = os.path.join(PROJECT_ROOT, 'playstation_games_with_concept_id.csv')
BASE_URL = "https://store.playstation.com/tr-tr/concept/{}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
MAX_EDITIONS = 5
# GÜNCELLEME: Aynı anda çalışacak maksimum işçi (thread) sayısı
MAX_WORKERS = 5


# --- VERİ İŞLEME VE VERİTABANI YARDIMCI FONKSİYONLARI ---

def setup_mongodb_connection() -> Tuple[MongoClient, Database]:
    """MongoDB Atlas'a bağlantı kurar ve veritabanı nesnesini döndürür."""
    if not MONGO_URI:
        raise Exception("HATA: MONGO_URI ortam değişkeni ayarlanmamış!")

    print("MongoDB Atlas'a bağlanılıyor...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    print(f"'{MONGO_DB_NAME}' veritabanına başarıyla bağlanıldı.")
    return client, db

def clean_price(price_text: Optional[str]) -> str:
    """Fiyat metnini temizler."""
    if not price_text:
        return 'N/A'
    return price_text.replace('\xa0', ' ').replace('TL', '').strip()



def insert_or_update_game(cursor: sqlite3.Cursor, game_data: Dict[str, Any], table_name: str):
    """Veritabanına tek bir oyun verisini ekler veya günceller."""
    columns = ', '.join(game_data.keys())
    placeholders = ', '.join(['?'] * len(game_data))
    query = f"INSERT OR REPLACE INTO '{table_name}' ({columns}) VALUES ({placeholders})"
    values = tuple(game_data.values())
    cursor.execute(query, values)


# --- WEB SCRAPING FONKSİYONLARI ---

def get_page_soup(url: str) -> Optional[BeautifulSoup]:
    """Verilen URL'den sayfa içeriğini alır ve BeautifulSoup nesnesi döndürür."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    except requests.exceptions.RequestException as e:
        # Paralel çalışmada hatanın hangi URL'den geldiğini bilmek önemlidir.
        # print(f"  -> HATA: Sayfa alınamadı. URL: {url}, Hata: {e}") # Bu çok fazla çıktı üretebilir.
        return None


def scrape_game_editions(soup: BeautifulSoup, default_name: str) -> List[Dict[str, str]]:
    editions_found = []
    i = 0
    while True:
        edition_article = soup.find('article', attrs={'data-qa': f'mfeUpsell#productEdition{i}'})
        if not edition_article:
            break
        edition_name_tag = edition_article.find('h3', attrs={'data-qa': lambda v: v and v.endswith('#editionName')})
        price_tag = edition_article.find('span', attrs={'data-qa': lambda v: v and v.endswith('#finalPrice')})
        edition_name = edition_name_tag.get_text(strip=True) if edition_name_tag else f"Bilinmeyen Sürüm {i + 1}"
        price = clean_price(price_tag.get_text()) if price_tag else 'N/A'
        editions_found.append({'name': edition_name, 'price': price})
        i += 1

    if editions_found:
        return editions_found

    main_price_tag = soup.find('span', attrs={'data-qa': 'mfeCtaMain#offer0#finalPrice'})
    main_title_tag = soup.find('h1', attrs={'data-qa': 'mfe-game-title#name'})
    edition_name = main_title_tag.get_text(strip=True) if main_title_tag else default_name

    if main_price_tag:
        editions_found.append({'name': edition_name, 'price': clean_price(main_price_tag.get_text())})
    else:
        free_tag = soup.find(
            lambda tag: tag.name == 'span' and tag.get_text(strip=True).lower() in ['ücretsiz', 'free', 'indir',
                                                                                    'download', 'oyna', 'play'])
        editions_found.append({'name': edition_name, 'price': 'Ücretsiz/Dahil' if free_tag else 'N/A'})

    return editions_found


def prepare_document_for_mongodb(concept_id: str, game_name: str, editions: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Scrape edilen veriyi, 'price_history' koleksiyonuna eklenecek
    BSON dokümanı formatına dönüştürür.
    """
    now_iso = datetime.now().isoformat() + "Z"

    # Sürümleri (editions) istediğimiz {name, price} formatında bir listeye dönüştür.
    # scrape_game_editions zaten bu formatta döndürdüğü için ek işlem gerekmiyor.

    price_document = {
        "gameId": concept_id,  # 'games' koleksiyonundaki _id'ye referans
        "snapshotDate": now_iso,  # Verinin çekildiği anın zaman damgası (ISO formatında)
        "editions": editions  # Sürümlerin olduğu dizi [{name: "...", price: "..."}, ...]
    }
    return price_document


def run_scraper_task():
    """Ana fonksiyon, görevleri paralel olarak yürütür ve sonuçları MongoDB'ye yazar."""
    if not os.path.exists(INPUT_CSV):
        print(f"HATA: Girdi dosyası bulunamadı: '{INPUT_CSV}'")
        return

    client, db = None, None  # Bağlantıyı en başta None olarak tanımla
    try:
        # YENİ: MongoDB bağlantısını kur.
        client, db = setup_mongodb_connection()
        price_collection = db['price_history']  # 'price_history' koleksiyonunu seç
    except Exception as e:
        print(f"Veritabanı bağlantı hatası: {e}")
        return  # Bağlantı kurulamazsa işlemi durdur

    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        games_to_scrape = list(csv.DictReader(f))

    total_games = len(games_to_scrape)
    print(f"Toplam {total_games} oyun bulundu. {MAX_WORKERS} işçi ile paralel olarak işlenecek...")

    processed_count = 0
    inserted_count = 0

    # ThreadPoolExecutor kullanarak görevleri paralel çalıştır
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_game = {executor.submit(process_game, game): game for game in games_to_scrape}

        for future in as_completed(future_to_game):
            game_name = future_to_game[future].get('name', 'Bilinmeyen Oyun')
            try:
                # DEĞİŞTİ: process_game artık doğrudan MongoDB dokümanını döndürecek
                price_document = future.result()
                if price_document:
                    # YENİ: Veriyi MongoDB'ye ekle
                    price_collection.insert_one(price_document)
                    inserted_count += 1

            except Exception as exc:
                print(f"  -> HATA: '{game_name}' işlenirken bir istisna oluştu: {exc}")
            finally:
                processed_count += 1
                if processed_count % 10 == 0 or processed_count == total_games:
                    print(f"[{processed_count}/{total_games}] oyun işlendi...")

    # YENİ: Sonuçları ve bağlantıyı kapatma
    if client:
        client.close()
        print("\nMongoDB bağlantısı kapatıldı.")

    print(f"\nİşlem tamamlandı! {inserted_count} adet fiyat bilgisi 'price_history' koleksiyonuna kaydedildi.")

# GÜNCELLEME: Tek bir oyunu işleyen fonksiyon (paralel çalıştırılacak)
def process_game(game: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Tek bir oyun için tüm scraping ve veri hazırlama adımlarını yürütür."""
    concept_id = game.get('concept_id')
    game_name = game.get('name', 'İsim Yok')

    if not concept_id:
        return None

    url = BASE_URL.format(concept_id)
    soup = get_page_soup(url)

    if soup:
        editions_list = scrape_game_editions(soup, game_name)
        # DEĞİŞTİ: Çağrılan fonksiyonun adı değişti.
        price_document = prepare_document_for_mongodb(concept_id, game_name, editions_list)
        return price_document


# --- ANA İŞLEM FONKSİYONU (GÜNCELLENMİŞ) ---




if __name__ == "__main__":
    run_scraper_task()
