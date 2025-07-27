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
INPUT_CSV = os.path.join(PROJECT_ROOT, 'playstation_games_with_concept_id.csv')
DATABASE_FILE = os.path.join(PROJECT_ROOT, 'playstation_games.db')
BASE_URL = "https://store.playstation.com/tr-tr/concept/{}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
MAX_EDITIONS = 5
# GÜNCELLEME: Aynı anda çalışacak maksimum işçi (thread) sayısı
MAX_WORKERS = 5


# --- VERİ İŞLEME VE VERİTABANI YARDIMCI FONKSİYONLARI ---

def clean_price(price_text: Optional[str]) -> str:
    """Fiyat metnini temizler."""
    if not price_text:
        return 'N/A'
    return price_text.replace('\xa0', ' ').replace('TL', '').strip()


def setup_database_and_table(table_name: str) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
    """Veritabanı bağlantısını kurar ve tarih damgalı tabloyu oluşturur."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    columns = ["concept_id TEXT PRIMARY KEY", "name TEXT"]
    for i in range(1, MAX_EDITIONS + 1):
        columns.append(f"surum_adi_{i} TEXT")
        columns.append(f"fiyat_{i} TEXT")

    create_table_query = f"CREATE TABLE IF NOT EXISTS '{table_name}' ({', '.join(columns)})"
    cursor.execute(create_table_query)
    conn.commit()
    print(f"Veritabanı '{DATABASE_FILE}' içinde '{table_name}' tablosu hazırlandı.")
    return conn, cursor


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


def prepare_data_for_db(concept_id: str, game_name: str, editions: List[Dict[str, str]]) -> Dict[str, Any]:
    db_row = {'concept_id': concept_id, 'name': game_name}
    for i in range(MAX_EDITIONS):
        surum_key, fiyat_key = f'surum_adi_{i + 1}', f'fiyat_{i + 1}'
        if i < len(editions):
            db_row[surum_key] = editions[i]['name']
            db_row[fiyat_key] = editions[i]['price']
        else:
            db_row[surum_key], db_row[fiyat_key] = None, None
    return db_row


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
        game_db_data = prepare_data_for_db(concept_id, game_name, editions_list)
        return game_db_data
    else:
        print(f"  -> UYARI: {game_name} (ID: {concept_id}) için sayfa içeriği alınamadı. Atlanıyor.")
        return None


# --- ANA İŞLEM FONKSİYONU (GÜNCELLENMİŞ) ---

def run_scraper_task():
    """Ana fonksiyon, görevleri paralel olarak yürütür."""
    if not os.path.exists(INPUT_CSV):
        print(f"HATA: Girdi dosyası bulunamadı: '{INPUT_CSV}'")
        return

    now = datetime.now()
    table_name = now.strftime("games_%d_%m_%Y_%H_%M")

    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        games_to_scrape = list(csv.DictReader(f))

    total_games = len(games_to_scrape)
    print(f"Toplam {total_games} oyun bulundu. {MAX_WORKERS} işçi ile paralel olarak işlenecek...")

    conn, cursor = setup_database_and_table(table_name)
    processed_count = 0

    try:
        # GÜNCELLEME: ThreadPoolExecutor kullanarak görevleri paralel çalıştır
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Her bir oyun için process_game fonksiyonunu çalıştır ve bir 'future' nesnesi al
            future_to_game = {executor.submit(process_game, game): game for game in games_to_scrape}

            # Görevler tamamlandıkça sonuçları işle
            for future in as_completed(future_to_game):
                game_name = future_to_game[future].get('name', 'Bilinmeyen Oyun')
                try:
                    game_db_data = future.result()
                    if game_db_data:
                        insert_or_update_game(cursor, game_db_data, table_name)

                except Exception as exc:
                    print(f"  -> HATA: '{game_name}' işlenirken bir istisna oluştu: {exc}")
                finally:
                    processed_count += 1
                    # Her 10 oyunda bir ilerleme durumu yazdır
                    if processed_count % 10 == 0 or processed_count == total_games:
                        print(f"[{processed_count}/{total_games}] oyun işlendi...")
    finally:
        conn.commit()
        conn.close()
        print(f"\nİşlem tamamlandı! Veriler '{DATABASE_FILE}' dosyasındaki '{table_name}' tablosuna kaydedildi.")


if __name__ == "__main__":
    run_scraper_task()
