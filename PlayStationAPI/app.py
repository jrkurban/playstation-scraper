import os
from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import json_util  # MongoDB'nin BSON formatını JSON'a çevirmek için çok önemli!

# .env dosyasındaki ortam değişkenlerini yükle
load_dotenv()

# Flask uygulamasını başlat
app = Flask(__name__)
# CORS'u aktif et, bu API'ye dışarıdan erişim izni verir.
CORS(app)

# MongoDB bağlantısını kur
MONGO_URI = os.getenv('MONGO_URI')
if not MONGO_URI:
    raise Exception("HATA: MONGO_URI ortam değişkeni bulunamadı!")

client = MongoClient(MONGO_URI)
db = client['GamesDB']  # Veritabanını seç
games_collection = db['games']
price_history_collection = db['price_history']


# --- API ENDPOINT'LERİ ---

@app.route("/api/games", methods=["GET"])
def get_all_games():
    """Tüm oyunları veritabanından çeker ve JSON olarak döndürür."""
    try:
        # Oyunları isme göre alfabetik sıralayarak bul
        games = list(games_collection.find().sort("name", 1))
        # BSON'u JSON formatına çevirip döndür. json_util kullanmak şart!
        return json_util.dumps(games), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/games/<string:game_id>/price", methods=["GET"])
def get_latest_price(game_id):
    """Belirli bir oyunun en son fiyat kaydını döndürür."""
    try:
        # Verilen game_id'ye ait kayıtları, tarihe göre tersten sırala ve ilkini al.
        latest_price_doc = price_history_collection.find_one(
            {"gameId": game_id},
            sort=[("snapshotDate", -1)]
        )

        if not latest_price_doc:
            return jsonify({"error": "Bu oyun için fiyat bilgisi bulunamadı."}), 404

        return json_util.dumps(latest_price_doc), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Bu blok, kodu doğrudan 'python app.py' ile çalıştırdığımızda
# Flask'ın test sunucusunu başlatır.
if __name__ == "__main__":
    app.run(debug=True, port=5001)  # Farklı bir port belirttim