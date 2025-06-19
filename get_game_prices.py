import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import sqlite3
from datetime import datetime

# --- AYARLAR ---
INPUT_CSV = 'playstation_games_with_concept_id.csv'
DATABASE_FILE = 'playstation_games.db'
# TABLE_NAME statik olarak kaldırıldı, dinamik olarak oluşturulacak.
BASE_URL = "https://store.playstation.com/tr-tr/concept/{}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
MAX_EDITIONS = 5


def clean_price(price_text):
    """Fiyat metnini temizler."""
    if not price_text:
        return 'N/A'
    return price_text.replace('\xa0', ' ').replace('TL', '').strip()


def setup_database_and_table(table_name):
    """Veritabanını hazırlar ve dinamik isimle tabloyu oluşturur."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    # Sütunları dinamik olarak oluştur
    columns = [
        "concept_id TEXT PRIMARY KEY",
        "name TEXT"
    ]
    for i in range(1, MAX_EDITIONS + 1):
        columns.append(f"surum_adi_{i} TEXT")
        columns.append(f"fiyat_{i} TEXT")

    create_table_query = f"CREATE TABLE IF NOT EXISTS '{table_name}' ({', '.join(columns)})"

    cursor.execute(create_table_query)
    conn.commit()
    print(f"Veritabanı '{DATABASE_FILE}' içinde '{table_name}' tablosu hazırlandı/doğrulandı.")
    return conn, cursor


def insert_or_update_game(cursor, game_data, table_name):
    """Veritabanına bir oyun verisini ekler veya günceller."""
    columns = ', '.join(game_data.keys())
    placeholders = ', '.join(['?'] * len(game_data))

    # 'INSERT OR REPLACE' ile aynı concept_id'ye sahip kayıtlar güncellenir.
    query = f"INSERT OR REPLACE INTO '{table_name}' ({columns}) VALUES ({placeholders})"

    values = tuple(game_data.values())
    cursor.execute(query, values)


def scrape_and_save_to_db():
    """Oyunları kazır ve dinamik isimli bir tabloya kaydeder."""
    if not os.path.exists(INPUT_CSV):
        print(f"HATA: Girdi dosyası bulunamadı: '{INPUT_CSV}'")
        return

    # --- Dinamik Tablo Adı Oluşturma ---
    now = datetime.now()
    # "19.06.2025-17:02" formatını "games_19_06_2025_17_02" şekline dönüştür
    table_name = now.strftime("games_%d_%m_%Y_%H_%M")

    conn, cursor = setup_database_and_table(table_name)

    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        games_to_scrape = list(reader)

    total_games = len(games_to_scrape)
    print(f"Toplam {total_games} oyun bulundu. Veriler çekiliyor...")

    for i, game in enumerate(games_to_scrape):
        concept_id = game.get('concept_id')
        game_name = game.get('name')

        if not concept_id:
            continue

        url = BASE_URL.format(concept_id)
        print(f"[{i + 1}/{total_games}] İşleniyor: {game_name} (ID: {concept_id})")

        current_game_data = {'concept_id': concept_id, 'name': game_name}

        try:
            response = requests.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            editions_found = []

            upsell_section = soup.find('div', attrs={'data-qa': 'mfeUpsell'})
            if upsell_section:
                edition_articles = upsell_section.find_all('article', attrs={
                    'data-qa': lambda v: v and v.startswith('mfeUpsell#productEdition')})
                for article in edition_articles:
                    edition_name_tag = article.find('h3', attrs={'data-qa': lambda v: v and v.endswith('#editionName')})
                    price_tag = article.find('span', attrs={'data-qa': lambda v: v and v.endswith('#finalPrice')})

                    edition_name = edition_name_tag.get_text(strip=True) if edition_name_tag else 'Bilinmeyen Sürüm'
                    price = clean_price(price_tag.get_text()) if price_tag else 'N/A'
                    editions_found.append({'name': edition_name, 'price': price})
            else:
                main_price_tag = soup.find('span', attrs={'data-qa': 'mfeCtaMain#offer0#finalPrice'})
                main_title_tag = soup.find('h1', attrs={'data-qa': 'mfe-game-title#name'})
                edition_name = main_title_tag.get_text(strip=True) if main_title_tag else game_name

                if main_price_tag:
                    price = clean_price(main_price_tag.get_text())
                    editions_found.append({'name': edition_name, 'price': price})
                else:
                    free_tag = soup.find(
                        lambda tag: tag.get_text(strip=True).lower() in ['ücretsiz', 'free', 'indir', 'download'])
                    price = 'Ücretsiz' if free_tag else 'N/A'
                    editions_found.append({'name': edition_name, 'price': price})

            for idx, edition in enumerate(editions_found):
                if idx < MAX_EDITIONS:
                    current_game_data[f'surum_adi_{idx + 1}'] = edition['name']
                    current_game_data[f'fiyat_{idx + 1}'] = edition['price']

            insert_or_update_game(cursor, current_game_data, table_name)

        except requests.exceptions.RequestException as e:
            print(f"  -> HATA: {game_name} sayfası alınamadı. Hata: {e}")

        # Her 10 oyunda bir veritabanına kaydet (performans için)
        if (i + 1) % 10 == 0:
            conn.commit()
            print(f"  -> {i + 1}. oyuna kadar olanlar veritabanına kaydedildi.")

        time.sleep(0.5)

    # Döngü sonunda kalan kayıtları da işle
    conn.commit()
    conn.close()
    print(
        f"\nİşlem başarıyla tamamlandı! Tüm veriler '{DATABASE_FILE}' dosyasındaki '{table_name}' tablosuna kaydedildi.")


# Betiği başlat
if __name__ == "__main__":
    scrape_and_save_to_db()