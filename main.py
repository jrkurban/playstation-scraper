import sqlite3
import os
from datetime import datetime

# --- AYARLAR ---
# Betiğin bulunduğu dizine göre veritabanı dosyasının yolunu belirler.
# Bu, betiği nerede çalıştırırsanız çalıştırın doğru dosyayı bulmasını sağlar.
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATABASE_FILE = os.path.join(PROJECT_ROOT,'playstation_games.db')  # Bir üst dizindeki db dosyası
MAX_EDITIONS = 5  # Kontrol edilecek maksimum sürüm sayısı


def get_tables(conn):
    """Veritabanındaki oyun tablolarını tarihe göre sıralı döndürür."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'games_%';")
    tables = []
    for table in cursor.fetchall():
        try:
            # Tablo adından tarih bilgisini çıkararak sıralama yap
            dt_obj = datetime.strptime(table[0], "games_%d_%m_%Y_%H_%M")
            tables.append((dt_obj, table[0]))
        except ValueError:
            continue
    tables.sort(key=lambda x: x[0])
    return [table[1] for table in tables]


def parse_price(price_str):
    """Fiyat metnini sayısal (float) bir değere dönüştürür."""
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


def compare_prices():
    """İki tabloyu karşılaştırır ve tüm sürümlerdeki fiyat düşüşlerini bulur."""
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

    old_data = fetch_data_as_dict(cursor, old_table)
    new_data = fetch_data_as_dict(cursor, new_table)
    price_drops = []

    # --- TEK DÖNGÜ İLE TÜM SÜRÜMLERİ KONTROL ETME ---
    for concept_id, new_game in new_data.items():
        if concept_id in old_data:
            old_game = old_data[concept_id]

            # 1'den 5'e kadar tüm fiyat ve sürüm sütunlarını kontrol et
            for i in range(1, MAX_EDITIONS + 1):
                fiyat_col = f'fiyat_{i}'
                surum_col = f'surum_adi_{i}'

                old_price_str = old_game.get(fiyat_col)
                new_price_str = new_game.get(fiyat_col)

                # Eğer herhangi bir sürümde veri yoksa bu adımı atla
                if old_price_str is None or new_price_str is None:
                    continue

                old_price_val = parse_price(old_price_str)
                new_price_val = parse_price(new_price_str)

                # Sadece her iki fiyat da geçerliyse ve yeni fiyat daha düşükse listeye ekle
                if old_price_val is not None and new_price_val is not None and new_price_val < old_price_val:
                    price_drops.append({
                        'name': new_game.get('name'),
                        'edition': new_game.get(surum_col, f'Sürüm {i}'),  # Sürüm adı yoksa varsayılan ata
                        'old_price': old_price_str,
                        'new_price': new_price_str,
                    })

    # --- Geliştirilmiş Sonuçları Yazdırma ---
    if price_drops:
        print(f"--- Fiyatı Düşen {len(price_drops)} Ürün/Sürüm Bulundu! ---")
        print(f"{'Oyun Adı':<45} | {'Sürüm':<35} | {'Eski Fiyat':>15} | {'Yeni Fiyat':>15}")
        print("-" * 120)
        for game in price_drops:
            print(f"{game['name']:<45} | {game['edition']:<35} | {game['old_price']:>15} | {game['new_price']:>15}")
    else:
        print("--- Seçilen tablolar arasında fiyatı düşen bir oyun veya sürüm bulunamadı. ---")

    conn.close()


if __name__ == "__main__":
    compare_prices()