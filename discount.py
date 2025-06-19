import sqlite3
import os

# --- AYARLAR ---
DATABASE_FILE = 'playstation_games.db'


def get_tables(conn):
    """Veritabanındaki tüm tabloların listesini döndürür."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]
    return tables


def parse_price(price_str):
    """
    '1.749,00' gibi bir metni sayısal bir değere (float) dönüştürür.
    'Ücretsiz' veya geçersiz metinleri de işler.
    """
    if price_str is None:
        return None

    price_str = price_str.strip().lower()

    if 'ücretsiz' in price_str or 'free' in price_str:
        return 0.0

    try:
        # Türk para formatını (1.749,00) standart float formatına (1749.00) çevir
        cleaned_str = price_str.replace('.', '').replace(',', '.')
        return float(cleaned_str)
    except (ValueError, TypeError):
        # Eğer metin "N/A", "Hata" gibi bir değerse veya sayıya çevrilemezse
        return None


def fetch_data_as_dict(cursor, table_name):
    """Bir tablodaki tüm veriyi concept_id'yi anahtar olarak kullanan bir sözlüğe çeker."""
    query = f"SELECT * FROM '{table_name}'"
    cursor.execute(query)

    # Sütun adlarını al
    columns = [description[0] for description in cursor.description]

    game_dict = {}
    for row in cursor.fetchall():
        row_dict = dict(zip(columns, row))
        concept_id = row_dict.get('concept_id')
        if concept_id:
            game_dict[concept_id] = row_dict

    return game_dict


def compare_prices():
    """İki tabloyu karşılaştırır ve fiyatı düşen oyunları listeler."""
    if not os.path.exists(DATABASE_FILE):
        print(f"HATA: Veritabanı dosyası bulunamadı: '{DATABASE_FILE}'")
        return

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    tables = get_tables(conn)
    if len(tables) < 2:
        print("HATA: Karşılaştırma yapmak için veritabanında en az iki tablo olmalıdır.")
        conn.close()
        return

    # Kullanıcıya tabloları listele ve seçim yapmasını iste
    print("Veritabanında bulunan tablolar:")
    for i, table_name in enumerate(tables):
        print(f"  {i + 1}: {table_name}")

    try:
        old_table_idx = int(input("Lütfen 'ESKİ' veriyi içeren tablonun numarasını girin: ")) - 1
        new_table_idx = int(input("Lütfen 'YENİ' veriyi içeren tablonun numarasını girin: ")) - 1

        if not (0 <= old_table_idx < len(tables) and 0 <= new_table_idx < len(tables)):
            raise ValueError("Geçersiz tablo numarası.")

        old_table = tables[old_table_idx]
        new_table = tables[new_table_idx]

    except (ValueError, IndexError):
        print("Hatalı giriş. Lütfen listedeki numaralardan birini girin.")
        conn.close()
        return

    print(f"\nKarşılaştırılıyor: '{old_table}' (Eski) vs '{new_table}' (Yeni)\n")

    # Verileri çek
    old_data = fetch_data_as_dict(cursor, old_table)
    new_data = fetch_data_as_dict(cursor, new_table)

    price_drops = []

    # Yeni verilerdeki her oyunu kontrol et
    for concept_id, new_game in new_data.items():
        if concept_id in old_data:
            old_game = old_data[concept_id]

            # Karşılaştırma için ilk sürümün fiyatını kullan (genellikle standart sürüm)
            old_price_str = old_game.get('fiyat_1')
            new_price_str = new_game.get('fiyat_1')

            old_price_val = parse_price(old_price_str)
            new_price_val = parse_price(new_price_str)

            # Sadece her ikisi de geçerli bir fiyatsa karşılaştır
            if old_price_val is not None and new_price_val is not None:
                if new_price_val < old_price_val:
                    price_drops.append({
                        'concept_id': concept_id,
                        'name': new_game.get('name'),
                        'old_price': old_price_str,
                        'new_price': new_price_str,
                        'old_edition': old_game.get('surum_adi_1'),
                        'new_edition': new_game.get('surum_adi_1')
                    })

    # --- Sonuçları Yazdır ---
    if price_drops:
        print(f"--- Fiyatı Düşen {len(price_drops)} Oyun Bulundu! ---")
        # Sütun başlıklarını yazdır
        print(f"{'Oyun Adı':<50} | {'Eski Fiyat':>15} | {'Yeni Fiyat':>15}")
        print("-" * 85)
        for game in price_drops:
            print(f"{game['name']:<50} | {game['old_price']:>15} | {game['new_price']:>15}")
    else:
        print("--- Seçilen tablolar arasında fiyatı düşen oyun bulunamadı. ---")

    conn.close()


# Betiği başlat
if __name__ == "__main__":
    compare_prices()