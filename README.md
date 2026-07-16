# Ev Çevresi Analizi (Belçika · OSM + DuckDB)

Bir **adres** (veya koordinat) verdiğinizde, çevredeki **okul, market, sağlık, ulaşım, park ve spor** noktalarını (POI) bulur; her nokta için **yürüme/araç mesafe–süre** tahmini yapar; kategori bazlı ve genel **0–10 yaşanabilirlik puanı** hesaplar. Sonuçları hem **web arayüzünde** (interaktif harita + skor paneli) hem de **komut satırında** (konsol + `map.html`) sunar.

Arayüz **üç dilli**: Türkçe / İngilizce / Felemenkçe.

Veri kaynağı OpenStreetMap'in Belçika verisi. Analiz çevrimdışı parquet cache üzerinden çalışır; internet yalnızca (1) adres çözümleme (Nominatim) ve (2) web arayüzünün tarayıcı bileşenleri (harita, React) için gerekir.

---

## Mimari

```
data/belgium-latest.osm.pbf                (ham OSM verisi, tek seferlik indirilir)
        │  build_poi_cache.py               → cache/be_poi.parquet        (node POI'ler)
        │  build_poi_poly_cache_osmium.py   → cache/be_poi_poly.parquet   (alan/polygon POI centroid'leri)
        ▼
app_duckdb.py : run_analysis()             ← analiz çekirdeği (DuckDB ile parquet'i sorgular)
        ├── main()   → CLI: konsol çıktısı + map.html (folium)
        └── server.py → Flask JSON API + statik frontend
                          static/index.html + support.js  (Leaflet haritalı SPA)
```

- **Parquet + DuckDB:** Parquet dosyaları salt-okunur "tablolar", DuckDB ise onları SQL ile sorgulayan gömülü motor. Ayrı bir veritabanı sunucusu yok; sorgular bounding-box ön filtresi + Haversine mesafesi ile milisaniyeler içinde çalışır.
- **Tek çekirdek:** Hem CLI hem web API aynı `run_analysis()` fonksiyonunu kullanır (puanlama tek yerde).

## Dosyalar

| Dosya | Görev |
|---|---|
| `belgium-location/app_duckdb.py` | Analiz çekirdeği (`run_analysis`) + CLI (`main`) + folium harita |
| `belgium-location/server.py` | Flask web API (`/api/analyze`, `/api/health`) + statik frontend servis |
| `belgium-location/build_poi_cache.py` | PBF → node POI cache (`be_poi.parquet`) |
| `belgium-location/build_poi_poly_cache_osmium.py` | PBF → polygon POI centroid cache (`be_poi_poly.parquet`) |
| `belgium-location/static/index.html` | Frontend (Claude Design çıktısı, üç dilli SPA) |
| `belgium-location/static/support.js` | Frontend runtime (React'i CDN'den yükler) |
| `docs/FRONTEND_DESIGN_BRIEF.md` | Tasarım brief'i (i18n, ekranlar, API sözleşmesi) |
| `docs/FRONTEND_PLAN.md` | Yol haritası ve yapılacaklar |

---

## 1) Kurulum

Gereksinim: **Python 3.11** (osmium wheel'ı sorunsuz gelir).

```powershell
cd belgium-location
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -r requirements.txt
```

macOS / Linux:

```bash
cd belgium-location
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt
```

## 2) Veri ve cache (tek seferlik)

`data/` içine Belçika PBF'ini indirin (~650 MB):

```powershell
curl.exe -L -o ..\data\belgium-latest.osm.pbf https://download.geofabrik.de/europe/belgium-latest.osm.pbf
```

İki cache'i üretin (CPU+disk yoğun; node cache ~25 dk, polygon ~5 dk sürebilir):

```powershell
python .\build_poi_cache.py            --pbf "..\data\belgium-latest.osm.pbf" --out "..\cache\be_poi.parquet"
python .\build_poi_poly_cache_osmium.py --pbf "..\data\belgium-latest.osm.pbf" --out "..\cache\be_poi_poly.parquet"
```

Bitince `cache/` içinde iki `.parquet` dosyası oluşur (konsolda `[DONE]`).

---

## 3) Web arayüzü

```powershell
# Cache repo kökündeyse yollarını ortam değişkeniyle verin:
$env:POI_NODES = (Resolve-Path ..\cache\be_poi.parquet).Path
$env:POI_POLYS = (Resolve-Path ..\cache\be_poi_poly.parquet).Path
python .\server.py
```

Tarayıcıda **http://127.0.0.1:5000** açın. Adres girip "Analiz et" deyin — harita, puanlar ve kategori kartları gelir. Sağ üstten **dil (TR/EN/NL)** ve **tema (açık/koyu)** değiştirilebilir.

> Web arayüzü ilk render'da React/Leaflet/font'ları CDN'den çeker → **internet gerekir**. İnternetsiz/kurumsal ortam için bu bağımlılıkları yerelde barındırmak gerekir (bkz. `docs/FRONTEND_PLAN.md`).

### API

`POST /api/analyze` — gövde: `{ "address"?, "lat"?, "lon"?, "radius"=2500, "topn"=5, "lang"="tr" }`

```json
{
  "display_address": "…", "lat": 50.876, "lon": 4.680, "radius": 2500, "lang": "tr",
  "overall": 7.4,
  "categories": [
    { "key": "market", "label": "Market", "score": 8.2, "count": 12, "nearest_m": 350,
      "has_hospital": null,
      "items": [ { "name": "Colruyt", "brand": "Colruyt", "lat": 50.877, "lon": 4.682,
                   "walk_m": 440, "walk_min": 6, "drive_m": 490, "drive_min": 1 } ] }
  ]
}
```

Hata durumları: `400` (adres bulunamadı / parametre hatası), `502` (geocode servisi), `503` (cache yok). Sonuçsuz bölge: `200` + `note` alanı. Mesajlar `lang`'e göre çevrilir.

`GET /api/health` — cache dosyalarının durumunu döndürür.

---

## 4) Komut satırı (harita üreten CLI)

```powershell
$env:POI_NODES = (Resolve-Path ..\cache\be_poi.parquet).Path   # opsiyonel; varsayılan ./cache/...
python .\app_duckdb.py --address "Tervuursesteenweg 147, 3001 Heverlee, Belgium" --radius 2500 `
  --nodes ..\cache\be_poi.parquet --polys ..\cache\be_poi_poly.parquet
start .\map.html
```

Koordinatla: `--lat 50.876182 --lon 4.680335` (adres yerine).

**Parametreler:** `--address` veya `--lat/--lon`; `--radius` (m, vars. 2500); `--topn` (kategori başına, vars. 5); `--nodes`, `--polys` (cache yolları).

---

## 5) Puanlama

- **Kategori içi sıralama:** POI türüne göre SQL ağırlıkları (`SCORES`, `app_duckdb.py`).
- **Kategori puanı (0–10):** yakınlık (doygunluk mesafesi `D0`) + adet doygunluğu (`Nsat`) + sağlıkta hastane bonusu (`SCORING`).
- **Genel puan:** kategori ağırlıkları (`OVERALL_WEIGHTS`) ile normalize edilmiş ortalama.
- **Mesafe/süre modeli (yaklaşık):** yürüme 4.8 km/s (dolaşıklık ×1.25), araç 35 km/s (×1.40) — kuş uçuşu mesafeye uygulanır. Gerçek rota süreleri için ileride OSRM/Valhalla entegrasyonu düşünülebilir.

Kategoriler, renkler, dil etiketleri ve puan parametreleri `app_duckdb.py` başındaki sözlüklerden ayarlanır.

## 6) Veriyi güncel tutma

Cache bir **anlık görüntüdür**. Güncellemek için PBF'i yeniden indirip iki builder'ı tekrar çalıştırın (Geofabrik günlük güncelleniyor). Her zaman canlı veri için Overpass tabanlı hibrit mod fikri `docs/FRONTEND_PLAN.md` içinde not edilmiştir.

## 7) Sık karşılaşılan sorunlar

- **Web'de "sunucu hatası" / API 503:** cache dosyaları bulunamıyor. `GET /api/health` ile kontrol edin; `POI_NODES`/`POI_POLYS` yollarını doğrulayın.
- **`osmium` kurulamadı:** Python 3.11 venv kullanın.
- **Adres bulunamadı:** adresi posta koduyla yazın (ör. "Bondgenotenlaan 1, 3000 Leuven").
- **Harita boş / arayüz yüklenmiyor:** internet (CDN) erişimini kontrol edin.

---

Veriler © OpenStreetMap katkıda bulunanları.
