# scripts/generate_discount_report.py

import os
from datetime import datetime
from pymongo import MongoClient
from pymongo.database import Database
from typing import Dict, Any, List, Optional, Tuple

# --- AYARLAR ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_MD_FILE = os.path.join(PROJECT_ROOT, 'DISCOUNTS.md')

# MongoDB ayarlarını ortam değişkenlerinden al (GitHub Actions Secrets için ideal)
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = "GamesDB"


def setup_mongodb_connection() -> Tuple[Optional[MongoClient], Optional[Database]]:
    """MongoDB Atlas'a bağlantı kurar ve veritabanı nesnesini döndürür."""
    if not MONGO_URI:
        print("HATA: MONGO_URI ortam değişkeni ayarlanmamış!")
        return None, None
    try:
        print("MongoDB Atlas'a bağlanılıyor...")
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        # Bağlantıyı test etmek için sunucuya bir ping gönderelim.
        client.admin.command('ping')
        print(f"'{MONGO_DB_NAME}' veritabanına başarıyla bağlanıldı.")
        return client, db
    except Exception as e:
        print(f"MongoDB bağlantı hatası: {e}")
        return None, None

def parse_price(price_str: Optional[str]) -> Optional[float]:
    """Fiyat metnini sayısal bir değere (float) dönüştürür."""
    if price_str is None: return None
    price_str = price_str.strip().lower()
    # Fiyat içermeyen ifadeleri doğrudan None olarak işaretle
    if any(tag in price_str for tag in ['ücretsiz', 'dahil', 'oyna', 'indir', 'n/a']):
        return 0.0
    try:
        # Örnek: "1.299,00" -> "1299.00"
        cleaned_str = price_str.replace('.', '').replace(',', '.')
        return float(cleaned_str)
    except (ValueError, TypeError):
        return None

def fetch_snapshot_data(db: Database, snapshot_date: str) -> Dict[str, Dict[str, Any]]:
    """Belirli bir zaman damgasındaki tüm fiyat verilerini gameId anahtarlı bir sözlüğe çeker."""
    price_documents = db['price_history'].find({"snapshotDate": snapshot_date})
    return {doc['gameId']: doc for doc in price_documents}

def generate_report():
    client, db = setup_mongodb_connection()
    if not client or not db:
        return

    # 1. Karşılaştırma yapılacak iki anlık görüntüyü (snapshot) bul
    # 'price_history' koleksiyonundaki tüm farklı snapshotDate değerlerini al
    try:
        distinct_dates = db['price_history'].distinct("snapshotDate")
    except Exception as e:
        print(f"Veritabanından tarihleri alırken hata oluştu: {e}")
        client.close()
        return

    if len(distinct_dates) < 2:
        print("Karşılaştırma için yeterli veri (en az 2 kazıma işlemi) bulunamadı.")
        with open(OUTPUT_MD_FILE, 'w', encoding='utf-8') as f:
            f.write("# PlayStation İndirim Raporu\n\n")
            f.write(f"Rapor Tarihi: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n")
            f.write("Karşılaştırma yapılacak yeterli veri bulunamadı.")
        client.close()
        return

    # Tarihleri sırala ve en son iki tanesini al
    distinct_dates.sort()
    previous_date, latest_date = distinct_dates[-2], distinct_dates[-1]
    print(f"Karşılaştırılıyor: '{previous_date}' (Eski) vs '{latest_date}' (Yeni)")

    # 2. Eski ve yeni verileri MongoDB'den çek
    old_data = fetch_snapshot_data(db, previous_date)
    new_data = fetch_snapshot_data(db, latest_date)

    # 3. Oyun isimlerini tek seferde çekmek için bir harita oluştur (daha verimli)
    games_collection = db['games']
    game_info_map = {game['_id']: game for game in games_collection.find({}, {'name': 1})}

    price_drops = []

    # 4. Yeni verileri dolaşarak indirimleri bul
    for game_id, new_game_doc in new_data.items():
        if game_id in old_data:
            old_game_doc = old_data[game_id]
            game_name = game_info_map.get(game_id, {}).get('name', 'Bilinmeyen Oyun')

            # Eski sürümleri hızlı arama için bir sözlüğe dönüştür
            old_editions_map = {e['name']: e for e in old_game_doc.get('editions', [])}

            for new_edition in new_game_doc.get('editions', []):
                # Eğer aynı isimde eski bir sürüm varsa karşılaştır
                if new_edition['name'] in old_editions_map:
                    old_edition = old_editions_map[new_edition['name']]

                    old_price_val = parse_price(old_edition.get('price'))
                    new_price_val = parse_price(new_edition.get('price'))

                    if old_price_val is not None and new_price_val is not None and new_price_val < old_price_val:
                        price_drops.append({
                            'name': game_name,
                            'edition': new_edition['name'],
                            'old_price': old_edition.get('price'),
                            'new_price': new_edition.get('price'),
                        })

    # 5. Sonuçları Markdown dosyasına yazdır
    with open(OUTPUT_MD_FILE, 'w', encoding='utf-8') as f:
        report_time = datetime.now().strftime('%d.%m.%Y %H:%M')
        f.write("# PlayStation İndirim Raporu\n\n")
        f.write(f"**Rapor Tarihi:** {report_time}\n")
        f.write(f"**Karşılaştırılan Veriler:** En son iki veri seti\n\n")

        if price_drops:
            # İndirimleri oyun adına göre sırala
            price_drops.sort(key=lambda x: x['name'])
            f.write(f"### Fiyatı Düşen Toplam {len(price_drops)} Ürün Bulundu!\n\n")
            f.write("| Oyun Adı | Sürüm | Eski Fiyat | Yeni Fiyat |\n")
            f.write("|---|---|---|---|\n")
            for game in price_drops:
                f.write(f"| {game['name']} | {game['edition']} | ~{game['old_price']}~ | **{game['new_price']}** |\n")
        else:
            f.write("### Fiyatı Düşen Yeni Bir Ürün Bulunamadı.\n")

    print(f"Rapor başarıyla '{OUTPUT_MD_FILE}' dosyasına yazıldı.")
    client.close()

if __name__ == "__main__":
    generate_report()
