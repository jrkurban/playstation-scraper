# scripts/generate_discount_report.py

import os
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from pymongo.database import Database
from typing import Dict, Any, List, Optional, Tuple

# --- AYARLAR ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_MD_FILE = os.path.join(PROJECT_ROOT, 'DISCOUNTS.md')
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = "GamesDB"
# Kaç günlük geçmişe bakılacağını belirle
LOOKBACK_DAYS = 7

# --- VERİTABANI VE YARDIMCI FONKSİYONLAR (DEĞİŞİKLİK YOK) ---

def setup_mongodb_connection() -> Tuple[Optional[MongoClient], Optional[Database]]:
    if not MONGO_URI:
        print("HATA: MONGO_URI ortam değişkeni ayarlanmamış!")
        return None, None
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        client.admin.command('ping')
        print("MongoDB Atlas'a başarıyla bağlanıldı.")
        return client, db
    except Exception as e:
        print(f"MongoDB bağlantı hatası: {e}")
        return None, None

def parse_price(price_str: Optional[str]) -> Optional[float]:
    if price_str is None: return None
    price_str = price_str.strip().lower()
    if any(tag in price_str for tag in ['ücretsiz', 'dahil', 'oyna', 'indir', 'n/a']):
        return 0.0
    try:
        cleaned_str = price_str.replace('.', '').replace(',', '.')
        return float(cleaned_str)
    except (ValueError, TypeError):
        return None

# --- YENİ VE GÜNCELLENMİŞ FONKSİYONLAR ---

def get_latest_snapshot_date(db: Database) -> Optional[str]:
    """Veritabanındaki en son anlık görüntü tarihini döndürür."""
    latest_doc = db['price_history'].find_one(sort=[("snapshotDate", -1)])
    return latest_doc['snapshotDate'] if latest_doc else None

def fetch_data_by_snapshot_date(db: Database, snapshot_date_iso: str) -> Dict[str, Dict[str, Any]]:
    """Belirtilen ISO tarihine sahip tüm fiyat verilerini çeker."""
    price_documents = db['price_history'].find({"snapshotDate": snapshot_date_iso})
    return {doc['gameId']: doc for doc in price_documents}

def fetch_price_history_for_game(db: Database, game_id: str, start_date: datetime) -> List[Dict[str, Any]]:
    """Belirli bir oyun için verilen tarihten bugüne kadarki fiyat geçmişini getirir."""
    query = {
        "gameId": game_id,
        "snapshotDate": {"$gte": start_date.isoformat() + "Z"}
    }
    # En yeniden eskiye doğru sıralı
    return list(db['price_history'].find(query).sort("snapshotDate", -1))


def generate_report():
    client, db = setup_mongodb_connection()
    if client is None or db is None:
        return

    # 1. En son veri setini (bugünün verisi) bul ve çek
    latest_date_iso = get_latest_snapshot_date(db)
    if not latest_date_iso:
        print("Veritabanında hiç veri bulunamadı.")
        client.close()
        return
    
    # Tarih kontrolü: Eğer en son veri bugüne ait değilse bilgi ver
    latest_date_obj = datetime.fromisoformat(latest_date_iso.replace('Z', '+00:00'))
    if latest_date_obj.date() < datetime.now(timezone.utc).date():
        print(f"UYARI: En son veri {latest_date_obj.strftime('%d.%m.%Y')} tarihine ait. Rapor bu veriye göre oluşturulacak.")
    else:
        print(f"En son veri tarihi: {latest_date_iso}")

    current_data = fetch_data_by_snapshot_date(db, latest_date_iso)
    
    # 2. Karşılaştırma için başlangıç tarihini belirle
    lookback_start_date = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    # 3. Oyun isimlerini tek seferde çekmek için bir harita oluştur
    game_info_map = {game['_id']: game for game in db['games'].find({}, {'name': 1})}
    price_drops = []

    # 4. Bugünkü her oyun için geçmişi kontrol et
    for game_id, current_game_doc in current_data.items():
        game_name = game_info_map.get(game_id, {}).get('name', 'Bilinmeyen Oyun')
        
        # Bu oyunun son 7 günlük fiyat geçmişini çek
        history = fetch_price_history_for_game(db, game_id, lookback_start_date)

        for current_edition in current_game_doc.get('editions', []):
            current_price_val = parse_price(current_edition.get('price'))
            if current_price_val is None:
                continue

            reference_price = None
            reference_price_str = ""
            sale_start_date = latest_date_obj # Varsayılan olarak bugünün tarihi
            
            # Geçmişi (en yeniden eskiye) tara
            for historical_doc in history:
                for historical_edition in historical_doc.get('editions', []):
                    if historical_edition['name'] == current_edition['name']:
                        historical_price_val = parse_price(historical_edition.get('price'))
                        
                        if historical_price_val is not None and historical_price_val > current_price_val:
                            # İndirimsiz (daha yüksek) bir fiyat bulduk!
                            reference_price = historical_price_val
                            reference_price_str = historical_edition.get('price')
                            # İndirim, bu kayıttan sonraki kayıtta başlamıştır.
                            # `history` listesi tersten sıralı olduğu için, döngüdeki bir sonraki eleman
                            # aslında zamandaki bir önceki elemandır. Bu yüzden bu tarih doğru.
                            sale_start_date = datetime.fromisoformat(historical_doc['snapshotDate'].replace('Z', '+00:00'))
                        
                        # Referans fiyatı bulduysak, daha fazla geriye gitmeye gerek yok.
                        if reference_price is not None:
                            break
                if reference_price is not None:
                    break
            
            # Eğer bir referans fiyatı bulduysak (yani bir indirim varsa)
            if reference_price is not None:
                duration = (datetime.now(timezone.utc) - sale_start_date).days
                
                price_drops.append({
                    'name': game_name,
                    'edition': current_edition['name'],
                    'old_price': reference_price_str,
                    'new_price': current_edition.get('price'),
                    'duration_days': duration
                })

    # 5. Sonuçları Markdown dosyasına yazdır
    with open(OUTPUT_MD_FILE, 'w', encoding='utf-8') as f:
        report_time = datetime.now().strftime('%d.%m.%Y %H:%M')
        f.write("# PlayStation İndirim Raporu\n\n")
        f.write(f"**Rapor Tarihi:** {report_time}\n")
        f.write(f"**Not:** Fiyatlar son **{LOOKBACK_DAYS} gün** içindeki en yüksek fiyatlarla karşılaştırılmıştır.\n\n")

        if price_drops:
            price_drops.sort(key=lambda x: x['name'])
            f.write(f"### Fiyatı Düşen Toplam {len(price_drops)} Ürün Bulundu!\n\n")
            f.write("| Oyun Adı | Sürüm | Eski Fiyat | Yeni Fiyat | İndirim Süresi |\n")
            f.write("|---|---|---|---|---|\n")
            for game in price_drops:
                duration_text = f"~{game['duration_days']} gündür"
                if game['duration_days'] == 0:
                    duration_text = "**Bugün!**"
                elif game['duration_days'] == 1:
                    duration_text = "1 gündür"
                
                f.write(f"| {game['name']} | {game['edition']} | ~{game['old_price']}~ | **{game['new_price']}** | {duration_text} |\n")
        else:
            f.write("### Son 7 Gün İçinde Yeni Bir İndirim Tespit Edilmedi.\n")

    print(f"Rapor başarıyla '{OUTPUT_MD_FILE}' dosyasına yazıldı.")
    client.close()

if __name__ == "__main__":
    generate_report()
