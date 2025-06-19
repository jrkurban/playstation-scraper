import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import re

# --- AYARLAR ---
INPUT_CSV = 'playstation_games_with_concept_id.csv'
OUTPUT_CSV = 'playstation_games_with_prices_and_editions.csv'
BASE_URL = "https://store.playstation.com/tr-tr/concept/{}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
# Maksimum kaç sürüm için sütun oluşturulacağı
MAX_EDITIONS = 5


def clean_price(price_text):
    """Fiyat metnini temizler (örn: '2.099,00 TL' -> '2.099,00')."""
    if not price_text:
        return 'N/A'
    #   karakterini (\xa0) ve 'TL' metnini kaldırır.
    return price_text.replace('\xa0TL', '').strip()


def scrape_game_editions_and_prices():
    """
    CSV'den oyun listesini okur, her oyunun sürüm adlarını ve fiyatlarını kazır
    ve yeni bir CSV'ye yazar.
    """
    # 1. Girdi CSV dosyasını oku
    if not os.path.exists(INPUT_CSV):
        print(f"HATA: Girdi dosyası bulunamadı: '{INPUT_CSV}'")
        print(
            "Lütfen bu betiği bir önceki adımda oluşturulan CSV dosyasıyla aynı klasörde çalıştırdığınızdan emin olun.")
        return

    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        games_to_scrape = list(reader)

    print(f"Toplam {len(games_to_scrape)} oyun bulundu. Sürüm ve fiyat bilgileri çekiliyor...")

    final_data = []

    # 2. Her oyun için döngü başlat
    for i, game in enumerate(games_to_scrape):
        concept_id = game.get('concept_id')
        game_name = game.get('name')

        if not concept_id:
            continue

        url = BASE_URL.format(concept_id)
        print(f"[{i + 1}/{len(games_to_scrape)}] İşleniyor: {game_name} (ID: {concept_id})")

        game_data = {'concept_id': concept_id, 'name': game_name}

        try:
            # 3. Oyunun sayfasını indir
            response = requests.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # 4. Sürümleri ve fiyatları bul
            editions_found = []

            # "Sürümler" (Editions) bölümünü bul
            upsell_section = soup.find('div', attrs={'data-qa': 'mfeUpsell'})

            if upsell_section:
                # Bölümdeki her bir sürüm (article) için döngü
                edition_articles = upsell_section.find_all('article', attrs={
                    'data-qa': lambda x: x and x.startswith('mfeUpsell#productEdition')})
                for article in edition_articles:
                    edition_name_tag = article.find('h3', attrs={'data-qa': lambda x: x and x.endswith('#editionName')})
                    price_tag = article.find('span', attrs={'data-qa': lambda x: x and x.endswith('#finalPrice')})

                    if edition_name_tag and price_tag:
                        edition_name = edition_name_tag.get_text(strip=True)
                        price = clean_price(price_tag.get_text())
                        editions_found.append({'name': edition_name, 'price': price})
            else:
                # "Sürümler" bölümü yoksa, ana fiyatı ve başlığı almayı dene
                main_title_tag = soup.find('h1', attrs={'data-qa': 'mfe-game-title#name'})
                main_price_tag = soup.find('span', attrs={'data-qa': 'mfeCtaMain#offer0#finalPrice'})

                edition_name = main_title_tag.get_text(strip=True) if main_title_tag else game_name

                if main_price_tag:
                    price = clean_price(main_price_tag.get_text())
                    editions_found.append({'name': edition_name, 'price': price})
                else:
                    # Fiyat yoksa ücretsiz olup olmadığını kontrol et
                    free_tag = soup.find('span', {'data-qa': 'mfeCtaMain#offer0#discountDescriptor'})
                    if free_tag and free_tag.get_text(strip=True).lower() in ['ücretsiz', 'free', 'indir', 'download']:
                        editions_found.append({'name': edition_name, 'price': 'Ücretsiz'})
                    else:
                        editions_found.append({'name': edition_name, 'price': 'N/A'})

            # Toplanan verileri ana listeye ekle
            for idx, edition in enumerate(editions_found):
                if idx < MAX_EDITIONS:
                    game_data[f'surum_adi_{idx + 1}'] = edition['name']
                    game_data[f'fiyat_{idx + 1}'] = edition['price']

            final_data.append(game_data)

        except requests.exceptions.RequestException as e:
            print(f"  -> HATA: {game_name} sayfası alınamadı. Hata: {e}")
            game_data['surum_adi_1'] = 'Hata'
            game_data['fiyat_1'] = 'Hata'
            final_data.append(game_data)

        # Sunucuyu yormamak için her istek arasında 1 saniye bekle
        time.sleep(1)

    # 5. Sonuçları yeni bir CSV dosyasına yaz
    print(f"\nTarama tamamlandı. Veriler '{OUTPUT_CSV}' dosyasına yazılıyor...")

    # CSV başlıklarını oluştur
    fieldnames = ['concept_id', 'name']
    for i in range(1, MAX_EDITIONS + 1):
        fieldnames.append(f'surum_adi_{i}')
        fieldnames.append(f'fiyat_{i}')

    try:
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(final_data)

        print(f"İşlem başarıyla tamamlandı! '{OUTPUT_CSV}' dosyası oluşturuldu.")
    except IOError as e:
        print(f"Dosyaya yazma hatası: {e}")


# Betiği başlat
scrape_game_editions_and_prices()
