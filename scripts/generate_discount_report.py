# scripts/generate_discount_report.py

import sqlite3
import os
from datetime import datetime
from zoneinfo import ZoneInfo # Python 3.9+ için standart kütüphane

# --- AYARLAR ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_FILE = os.path.join(PROJECT_ROOT, 'playstation_games.db')
OUTPUT_MD_FILE = os.path.join(PROJECT_ROOT, 'DISCOUNTS.md')
MAX_EDITIONS = 5  # Kontrol edilecek maksimum sürüm sayısı
ISTANBUL_TZ = ZoneInfo("Europe/Istanbul") # İstanbul saat dilimi

def get_tables(conn):
    """Veritabanındaki tarih formatına uyan tabloları tarihe göre sıralı döndürür."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'games_%';")
    tables = []
    for table in cursor.fetchall():
        try:
            dt_obj = datetime.strptime(table[0], "games_%d_%m_%Y_%H_%M")
            tables.append((dt_obj, table[0]))
        except ValueError:
            continue
    tables.sort(key=lambda x: x[0])
    return [table[1] for table in tables]

def parse_price(price_str):
    """Fiyat metnini sayısal bir değere (float) dönüştürür."""
    if price_str is None: return None
    price_str = price_str.strip().lower()
    if 'ücretsiz' in price_str: return 0.0
    try:
        cleaned_str = price_str.replace('.', '').replace(',', '.')
        return float(cleaned_str)
    except (ValueError, TypeError):
        return None

def fetch_data_as_dict(cursor, table_name):
    """Bir tablodaki veriyi concept_id anahtarlı bir sözlüğe çeker."""
    query = f"SELECT * FROM '{table_name}'"
    cursor.execute(query)
    columns = [description[0] for description in cursor.description]
    return {row[0]: dict(zip(columns, row)) for row in cursor.fetchall()}

def generate_report():
    if not os.path.exists(DATABASE_FILE):
        print(f"HATA: Veritabanı dosyası bulunamadı: '{DATABASE_FILE}'")
        return

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    tables = get_tables(conn)
    if len(tables) < 2:
        print("Karşılaştırma için yeterli tablo (en az 2) bulunamadı.")
        # Yine de rapor dosyasını oluşturup bilgi notu düşelim
        with open(OUTPUT_MD_FILE, 'w', encoding='utf-8') as f:
            f.write("# PlayStation İndirim Raporu\n\n")
            f.write(f"Rapor Tarihi: {datetime.now(ISTANBUL_TZ).strftime('%d.%m.%Y %H:%M %Z')}\n\n")
            f.write("Karşılaştırma yapılacak yeterli veri (en az 2 kazıma işlemi) bulunamadı.")
        conn.close()
        return

    old_table, new_table = tables[-2], tables[-1]
    print(f"Karşılaştırılıyor: '{old_table}' (Eski) vs '{new_table}' (Yeni)")

    old_data = fetch_data_as_dict(cursor, old_table)
    new_data = fetch_data_as_dict(cursor, new_table)
    price_drops = []

    # --- REFAKTÖR EDİLMİŞ DÖNGÜ ---
    # Tüm fiyat sütunlarını tek bir döngüde kontrol et
    for concept_id, new_game in new_data.items():
        if concept_id in old_data:
            old_game = old_data[concept_id]
            
            for i in range(1, MAX_EDITIONS + 1):
                fiyat_col = f'fiyat_{i}'
                surum_col = f'surum_adi_{i}'

                # Eğer bu sürüm için veri yoksa, sonraki sürüme geç
                if fiyat_col not in new_game or fiyat_col not in old_game:
                    continue

                old_price_val = parse_price(old_game.get(fiyat_col))
                new_price_val = parse_price(new_game.get(fiyat_col))

                if old_price_val is not None and new_price_val is not None and new_price_val < old_price_val:
                    price_drops.append({
                        'name': new_game.get('name'),
                        'edition': new_game.get(surum_col, 'Standart Sürüm'),
                        'old_price': old_game.get(fiyat_col),
                        'new_price': new_game.get(fiyat_col),
                    })

    # --- Sonuçları Markdown dosyasına yazdır ---
    with open(OUTPUT_MD_FILE, 'w', encoding='utf-8') as f:
        now_istanbul = datetime.now(ISTANBUL_TZ)
        f.write("# PlayStation İndirim Raporu\n\n")
        f.write(f"**Rapor Tarihi:** {now_istanbul.strftime('%d.%m.%Y %H:%M %Z')}\n")
        f.write(f"**Karşılaştırılan Veriler:** `{old_table}` ve `{new_table}`\n\n")

        if price_drops:
            f.write(f"### Fiyatı Düşen Toplam {len(price_drops)} Ürün Bulundu!\n\n")
            f.write("| Oyun Adı | Sürüm | Eski Fiyat | Yeni Fiyat |\n")
            f.write("|---|---|---|---|\n")
            for game in price_drops:
                f.write(f"| {game['name']} | {game['edition']} | ~{game['old_price']}~ | **{game['new_price']}** |\n")
        else:
            f.write("### Fiyatı Düşen Yeni Bir Ürün Bulunamadı.\n")

    print(f"Rapor başarıyla '{OUTPUT_MD_FILE}' dosyasına yazıldı.")
    conn.close()

if __name__ == "__main__":
    generate_report()
