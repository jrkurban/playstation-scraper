# scripts/generate_discount_report.py
import sqlite3
import os
from datetime import datetime

# Proje kök dizinini dinamik olarak bul
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_FILE = os.path.join(PROJECT_ROOT, 'playstation_games.db')
OUTPUT_MD_FILE = os.path.join(PROJECT_ROOT, 'DISCOUNTS.md')  # Rapor dosyası


def get_tables(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'games_%';")
    tables = []
    for table in cursor.fetchall():
        try:
            # Tablo adından tarih parse etmeyi dene
            dt_obj = datetime.strptime(table[0], "games_%d_%m_%Y_%H_%M")
            tables.append((dt_obj, table[0]))
        except ValueError:
            # Format uymuyorsa atla
            continue
    # Tarihe göre sırala (en yeni en sonda olacak)
    tables.sort(key=lambda x: x[0])
    return [table[1] for table in tables]


def parse_price(price_str):
    if price_str is None: return None
    price_str = price_str.strip().lower()
    if 'ücretsiz' in price_str: return 0.0
    try:
        cleaned_str = price_str.replace('.', '').replace(',', '.')
        return float(cleaned_str)
    except (ValueError, TypeError):
        return None


def fetch_data_as_dict(cursor, table_name):
    query = f"SELECT * FROM '{table_name}'"
    cursor.execute(query)
    columns = [description[0] for description in cursor.description]
    game_dict = {row[0]: dict(zip(columns, row)) for row in cursor.fetchall()}
    return game_dict


def generate_report():
    if not os.path.exists(DATABASE_FILE):
        print(f"HATA: Veritabanı dosyası bulunamadı: '{DATABASE_FILE}'")
        return

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    tables = get_tables(conn)
    if len(tables) < 2:
        print("Karşılaştırma için yeterli tablo (en az 2) bulunamadı.")
        with open(OUTPUT_MD_FILE, 'w', encoding='utf-8') as f:
            f.write("# PlayStation İndirim Raporu\n\n")
            f.write(f"Rapor Tarihi: {datetime.now().strftime('%d-%m-%Y %H:%M')}\n\n")
            f.write("Karşılaştırma yapılacak yeterli veri (en az 2 kazıma işlemi) bulunamadı.")
        conn.close()
        return

    # En son iki tabloyu al
    old_table = tables[-2]
    new_table = tables[-1]

    print(f"Karşılaştırılıyor: '{old_table}' (Eski) vs '{new_table}' (Yeni)")

    old_data = fetch_data_as_dict(cursor, old_table)
    new_data = fetch_data_as_dict(cursor, new_table)
    price_drops = []

    for concept_id, new_game in new_data.items():
        if concept_id in old_data:
            old_game = old_data[concept_id]
            old_price_val = parse_price(old_game.get('fiyat_1'))
            new_price_val = parse_price(new_game.get('fiyat_1'))

            if old_price_val is not None and new_price_val is not None and new_price_val < old_price_val:
                price_drops.append({
                    'name': new_game.get('name'),
                    'old_price': old_game.get('fiyat_1'),
                    'new_price': new_game.get('fiyat_1'),
                })

    # --- Sonuçları Markdown dosyasına yazdır ---
    with open(OUTPUT_MD_FILE, 'w', encoding='utf-8') as f:
        f.write("# PlayStation İndirim Raporu\n\n")
        f.write(f"Rapor Tarihi: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n")
        f.write(f"Karşılaştırılan Tablolar: `{old_table}` ve `{new_table}`\n\n")

        if price_drops:
            f.write(f"### Fiyatı Düşen Toplam {len(price_drops)} Oyun Bulundu!\n\n")
            f.write("| Oyun Adı | Eski Fiyat | Yeni Fiyat |\n")
            f.write("|---|---|---|\n")
            for game in price_drops:
                f.write(f"| {game['name']} | {game['old_price']} | **{game['new_price']}** |\n")
        else:
            f.write("### Fiyatı Düşen Yeni Bir Oyun Bulunamadı.\n")

    print(f"Rapor başarıyla '{OUTPUT_MD_FILE}' dosyasına yazıldı.")
    conn.close()


if __name__ == "__main__":
    generate_report()
