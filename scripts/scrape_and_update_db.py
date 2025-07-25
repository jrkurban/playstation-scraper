# /scripts/scrape_and_update_db.py

import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any

# --- PROJE DİZİNİNİ OTOMATİK BULMA ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- AYARLAR ---
INPUT_CSV = os.path.join(PROJECT_ROOT, 'playstation_games_with_concept_id.csv')
DATABASE_FILE = os.path.join(PROJECT_ROOT, 'playstation_games.db')
BASE_URL = "https://store.playstation.com/tr-tr/concept/{}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
# Veritabanına yazılacak maksimum sürüm sayısı
MAX_EDITIONS = 5

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
        print(f"  -> HATA: Sayfa alınamadı. URL: {url}, Hata: {e}")
        return None

def scrape_game_editions(soup: BeautifulSoup, default_name: str) -> List[Dict[str, str]]:
    """
    Bir oyun sayfasından tüm sürümleri ve fiyatlarını kazır.
    Sorumluluğu sadece veri kazımaktır.
    """
    editions_found = []

    # Öncelikli olarak birden çok sürüm içeren bölümü ara
    upsell_section = soup.find('div', attrs={'data-qa': 'mfeUpsell'})
    if upsell_section:
        edition_articles = upsell_section.find_all('article', attrs={'data-qa': lambda v: v and v.startswith('mfeUpsell#productEdition')})
        for article in edition_articles:
            edition_name_tag = article.find('h3', attrs={'data-qa': lambda v: v and v.endswith('#editionName')})
            price_tag = article.find('span', attrs={'data-qa': lambda v: v and v.endswith('#finalPrice')})
            editions_found.append({
                'name': edition_name_tag.get_text(strip=True) if edition_name_tag else 'Bilinmeyen Sürüm',
                'price': clean_price(price_tag.get_text()) if price_tag else 'N/A'
            })
    
    # Eğer çoklu sürüm bölümü yoksa, ana ürün bilgilerini ara
    if not editions_found:
        main_price_tag = soup.find('span', attrs={'data-qa': 'mfeCtaMain#offer0#finalPrice'})
        main_title_tag = soup.find('h1', attrs={'data-qa': 'mfe-game-title#name'})
        edition_name = main_title_tag.get_text(strip=True) if main_title_tag else default_name

        if main_price_tag:
            editions_found.append({'name': edition_name, 'price': clean_price(main_price_tag.get_text())})
        else:
            # Fiyat yoksa, ücretsiz veya indirilebilir mi diye kontrol et
            free_tag = soup.find(lambda tag: tag.name == 'span' and tag.get_text(strip=True).lower() in ['ücretsiz', 'free', 'indir', 'download'])
            editions_found.append({'name': edition_name, 'price': 'Ücretsiz' if free_tag else 'N/A'})
            
    return editions_found

def prepare_data_for_db(concept_id: str, game_name: str, editions: List[Dict[str, str]]) -> Dict[str, str]:
    """Kazınan veriyi veritabanı şemasına uygun bir sözlüğe dönüştürür."""
    db_row = {'concept_id': concept_id, 'name': game_name}
    for i in range(1, MAX_EDITIONS + 1):
        if i <= len(editions):
            edition = editions[i-1]
            db_row[f'surum_adi_{i}'] = edition['name']
            db_row[f'fiyat_{i}'] = edition['price']
        else:
            # Kalan sütunları boş olarak doldur
            db_row[f'surum_adi_{i}'] = None
            db_row[f'fiyat_{i}'] = None
    return db_row

# --- ANA İŞLEM FONKSİYONU ---

def run_scraper_task():
    """GitHub Actions tarafından çağrılacak ana fonksiyon."""
    if not os.path.exists(INPUT_CSV):
        print(f"HATA: Girdi dosyası bulunamadı: '{INPUT_CSV}'")
        return

    now = datetime.now()
    table_name = now.strftime("games_%d_%m_%Y_%H_%M")
    
    try:
        with open(INPUT_CSV, 'r', encoding='utf-8') as f:
            games_to_scrape = list(csv.DictReader(f))
    except FileNotFoundError:
        print(f"HATA: {INPUT_CSV} dosyası bulunamadı.")
        return

    total_games = len(games_to_scrape)
    print(f"Toplam {total_games} oyun bulundu. Veriler '{table_name}' tablosuna işlenecek...")

    conn, cursor = setup_database_and_table(table_name)

    try:
        for i, game in enumerate(games_to_scrape):
            concept_id = game.get('concept_id')
            game_name = game.get('name', 'İsim Yok')

            if not concept_id:
                continue

            print(f"[{i + 1}/{total_games}] İşleniyor: {game_name} (ID: {concept_id})")

            url = BASE_URL.format(concept_id)
            soup = get_page_soup(url)

            if soup:
                # 1. Veriyi kazı (Sadece bu işi yapan fonksiyona devret)
                editions_list = scrape_game_editions(soup, game_name)
                
                # 2. Veriyi veritabanı için hazırla (Sadece bu işi yapan fonksiyona devret)
                game_db_data = prepare_data_for_db(concept_id, game_name, editions_list)
                
                # 3. Veritabanına ekle
                insert_or_update_game(cursor, game_db_data, table_name)
            
            time.sleep(0.5) # Sunucuyu yormamak için bekleme

    finally:
        # Hata olsa bile veritabanı bağlantısını güvenli bir şekilde kapat
        conn.commit()
        conn.close()
        print(f"\nİşlem tamamlandı! Veriler '{DATABASE_FILE}' dosyasındaki '{table_name}' tablosuna kaydedildi.")


if __name__ == "__main__":
    run_scraper_task()
