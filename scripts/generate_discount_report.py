# scripts/generate_discount_report.py

import os
import json # YENİ: JSON dosyası için import
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from pymongo.database import Database
from typing import Dict, Any, List, Optional, Tuple

# --- AYARLAR ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_MD_FILE = os.path.join(PROJECT_ROOT, 'DISCOUNTS.md')
# YENİ: JSON çıktısının yolu
OUTPUT_JSON_FILE = os.path.join(PROJECT_ROOT, 'discounts.json')

# GÜVENLİK DÜZELTMESİ: MONGO_URI'yi her zaman ortam değişkenlerinden al
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = "GamesDB"
# Kaç günlük geçmişe bakılacağını belirle
LOOKBACK_DAYS = 7


# --- VERİTABANI VE YARDIMCI FONKSİYONLAR ---

def setup_mongodb_connection() -> Tuple[Optional[MongoClient], Optional[Database]]:
    if not MONGO_URI:
        print("HATA: MONGO_URI ortam değişkeni ayarlanmamış! Lütfen GitHub Secrets'ı kontrol edin.")
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


# --- ANA İŞLEM FONKSİYONLARI ---

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
    return list(db['price_history'].find(query).sort("snapshotDate", -1))


def get_all_histories_in_range(db: Database, start_date: datetime) -> Dict[str, List[Dict[str, Any]]]:
    """
    Belirtilen tarihten itibaren tüm fiyat geçmişini çeker ve oyun ID'sine göre gruplar.
    """
    print(f"Veritabanından {start_date.strftime('%Y-%m-%d')} tarihinden itibaren tüm veriler çekiliyor...")
    query = {"snapshotDate": {"$gte": start_date.isoformat() + "Z"}}
    all_docs = list(db['price_history'].find(query))

    game_histories = {}
    for doc in all_docs:
        game_id = doc['gameId']
        if game_id not in game_histories:
            game_histories[game_id] = []
        game_histories[game_id].append(doc)

    # Her oyunun geçmişini tarihe göre sırala (en eskiden en yeniye)
    for game_id in game_histories:
        game_histories[game_id].sort(key=lambda x: x['snapshotDate'])

    print(f"{len(game_histories)} oyun için fiyat geçmişi gruplandı.")
    return game_histories


def generate_report():
    client, db = setup_mongodb_connection()
    if client is None or db is None:
        return

    # 1. Geriye dönük karşılaştırma için başlangıç tarihini belirle.
    # 7 günlük düşüşleri bulmak için 8 gün öncesine ait veri gerekebilir.
    reference_start_date = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS + 1)

    # 2. İlgili aralıktaki tüm veriyi çek ve oyunlara göre grupla
    all_game_histories = get_all_histories_in_range(db, reference_start_date)

    if not all_game_histories:
        print("Veritabanında analiz edilecek veri bulunamadı.")
        client.close()
        return

    # 3. Oyun isimlerini tek seferde çekmek için bir harita oluştur
    game_info_map = {game['_id']: game for game in db['games'].find({}, {'name': 1})}

    # Son 7 gün içinde fiyatı düşenleri saklamak için
    recent_price_drops = {}

    # 4. Her oyunun geçmişini analiz et
    for game_id, history in all_game_histories.items():
        if len(history) < 2:
            continue  # Karşılaştırma için en az 2 kayıt gerekir.

        # Geçmişi eskiden yeniye doğru tara
        for i in range(1, len(history)):
            previous_doc = history[i - 1]
            current_doc = history[i]

            # Fiyat düşüşü olayının tarihini kontrol et
            drop_date = datetime.fromisoformat(current_doc['snapshotDate'].replace('Z', '+00:00'))

            # Eğer düşüş son 7 gün içinde değilse, bu oyunu daha fazla analiz etmeye gerek yok
            if (datetime.now(timezone.utc) - drop_date).days > LOOKBACK_DAYS:
                continue

            prev_editions = {e['name']: e for e in previous_doc.get('editions', [])}

            for current_edition in current_doc.get('editions', []):
                if current_edition['name'] in prev_editions:
                    prev_edition = prev_editions[current_edition['name']]

                    prev_price_val = parse_price(prev_edition.get('price'))
                    current_price_val = parse_price(current_edition.get('price'))

                    if prev_price_val is not None and current_price_val is not None and current_price_val < prev_price_val:
                        # BİR İNDİRİM OLAYI TESPİT EDİLDİ!
                        game_name = game_info_map.get(game_id, {}).get('name', 'Bilinmeyen Oyun')

                        # Aynı oyun/sürüm için birden fazla düşüş varsa en sonuncusunu tut
                        drop_key = f"{game_id}-{current_edition['name']}"
                        recent_price_drops[drop_key] = {
                            'name': game_name,
                            'edition': current_edition['name'],
                            'old_price': prev_edition.get('price'),
                            'new_price': current_edition.get('price'),
                            'drop_date': drop_date  # İndirimin olduğu günün tarihi
                        }

    # 5. Rapor için son listeyi oluştur
    final_drops_list = []
    for drop in recent_price_drops.values():
        duration = (datetime.now(timezone.utc).date() - drop['drop_date'].date()).days
        drop['duration_days'] = duration
        del drop['drop_date']  # Raporda bu alana gerek yok
        final_drops_list.append(drop)

    # 6A. Sonuçları JSON dosyasına yazdır (iOS Uygulaması için)
    with open(OUTPUT_JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_drops_list, f, ensure_ascii=False, indent=2)
    print(f"JSON raporu başarıyla '{OUTPUT_JSON_FILE}' dosyasına yazıldı.")

    # 6B. Sonuçları Markdown dosyasına yazdır
    with open(OUTPUT_MD_FILE, 'w', encoding='utf-8') as f:
        report_time = datetime.now().strftime('%d.%m.%Y %H:%M')
        f.write("# PlayStation İndirim Raporu\n\n")
        f.write(f"**Rapor Tarihi:** {report_time}\n")
        f.write(f"**Not:** Son **{LOOKBACK_DAYS} gün** içinde fiyatı yeni düşen ürünler listelenmiştir.\n\n")

        if final_drops_list:
            final_drops_list.sort(key=lambda x: x['name'])
            f.write(f"### Yeni İndirime Giren Toplam {len(final_drops_list)} Ürün Bulundu!\n\n")
            f.write("| Oyun Adı | Sürüm | Eski Fiyat | Yeni Fiyat | Ne Kadar Süredir İndirimde? |\n")
            f.write("|---|---|---|---|---|\n")
            for game in final_drops_list:
                days = game['duration_days']
                duration_text = f"{days} gündür"
                if days == 0:
                    duration_text = "**Bugün!**"
                elif days == 1:
                    duration_text = "1 gündür"

                f.write(
                    f"| {game['name']} | {game['edition']} | ~{game['old_price']}~ | **{game['new_price']}** | {duration_text} |\n")
        else:
            f.write(f"### Son {LOOKBACK_DAYS} Gün İçinde Yeni Bir İndirim Tespit Edilmedi.\n")

    print(f"Rapor başarıyla '{OUTPUT_MD_FILE}' dosyasına yazıldı.")
    client.close()


if __name__ == "__main__":
    generate_report()
