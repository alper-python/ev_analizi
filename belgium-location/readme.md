

# Belçika Ev Çevresi Analizi (OSM + DuckDB)

Adres verdiğinizde, yakın çevredeki **okul**, **market**, **sağlık**, **ulaşım**, **park**, **spor** POI’lerini bulur; **yürüme/araç mesafe–süre** hesaplar ve **harita** üretir.
İnternet gerektirmez (sadece ilk çalıştırmada **adres geocode** için Nominatim kullanır).

## 0) Kullanılan dosyalar

* **app\_duckdb.py** → Analiz ve harita (node + polygon cache birleştirir).
* **build\_poi\_cache.py** → **Node cache** üretir → `cache/be_poi.parquet`
* **build\_poi\_poly\_cache\_osmium.py** → **Polygon (area) cache** üretir → `cache/be_poi_poly.parquet`

> `app.py` ve `build_poi_poly_cache_pyrosm.py` eskidir; kullanılmaz.

---

## 1) Gereksinimler

* **Python 3.11** (önerilen; `osmium` wheel’ı sorunsuz gelir)
* Windows, macOS veya Linux (Windows için PowerShell komutları aşağıda)
* SSD’de birkaç GB boş alan (PBF + cache)

---

## 2) Klasör yapısı (öneri)

```
belgium-location/
  app_duckdb.py
  build_poi_cache.py
  build_poi_poly_cache_osmium.py
  data/
    belgium-latest.osm.pbf
  cache/
    be_poi.parquet
    be_poi_poly.parquet
  .venv/  (sanalkurulum)
```

---

## 3) Sanal ortam ve kurulum

### Windows (PowerShell)

```powershell
cd path\to\belgium-location
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
python -m pip install duckdb pyarrow pandas folium geopy osmium shapely
```

### macOS / Linux

```bash
cd /path/to/belgium-location
python3.11 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install duckdb pyarrow pandas folium geopy osmium shapely
```

> **Not:** Python 3.12 ile `osmium` bazen wheel bulamaz. O durumda 3.11 kullanın.

---

## 4) PBF dosyasını ekle

`data/` içine **belgium-latest.osm.pbf** dosyasını koyun. (Genelde “Geofabrik”’ten indiriliyor.)

---

## 5) Cache üretimi (tek seferlik)

> Bu adımlar **ülke genelinde** tek kez yapılır; sonra sorgular çok hızlıdır.

### 5.1 Node cache

```powershell
python .\build_poi_cache.py --pbf ".\data\belgium-latest.osm.pbf" --out ".\cache\be_poi.parquet"
```

### 5.2 Polygon (area) cache

Büyük marketler (Colruyt/Delhaize) ve hastaneler (UZ Gasthuisberg gibi) genelde **alan** olarak etiketli; bu yüzden polygon cache şart.

```powershell
python .\build_poi_poly_cache_osmium.py --pbf ".\data\belgium-latest.osm.pbf" --out ".\cache\be_poi_poly.parquet"
```

> İpuçları:
>
> * İlk çalıştırma **uzun** sürebilir (CPU+disk yoğun).
> * İşlem biterken konsolda **\[DONE] …** görürsünüz.
> * `cache/` içinde iki dosya oluşmalı:
>
>   * `be_poi.parquet` (node)
>   * `be_poi_poly.parquet` (polygon)

---

## 6) Analizi çalıştırma

**Temel:**

```powershell
$nodes = (Resolve-Path .\cache\be_poi.parquet).Path
$polys = (Resolve-Path .\cache\be_poi_poly.parquet).Path

python .\app_duckdb.py `
  --address "Tervuursesteenweg 147, 3001 Heverlee, Belgium" `
  --radius 2500 `
  --nodes "$nodes" `
  --polys "$polys"

start .\map.html
```

**Alternatif:** Koordinatla çalıştırma

```powershell
python .\app_duckdb.py --lat 50.876182 --lon 4.680335 --radius 2500 --nodes "$nodes" --polys "$polys"
start .\map.html
```

> `app_duckdb.py`, node veya polygon cache’ten biri eksikse **otomatik** sadece olanı kullanarak devam eder (fallback).

---

## 7) Çıktılar

* **Konsol:** Her kategori için TOP-N liste + yürüme/araç **mesafe ve süre**.
* **Harita:** Kök dizine `map.html` kaydeder.

  * Marker renkleri kategorilere göre (altta **Legenda** kutusu var).
  * Her marker popup’ında yürüme/araç mesafe-süre.

---

## 8) Parametreler

* `--address` veya `--lat --lon`
* `--radius` (metre) → varsayılan 2500
* `--topn` → her kategori için döndürülecek öğe sayısı (varsayılan 5)
* `--nodes`, `--polys` → cache dosyalarının yolları

**Hız/mesafe modeli (yaklaşık):**

* Yürüme hızı **4.8 km/s**, dolaşıklık (circuity) **1.25**
* Araç hızı **35 km/s**, dolaşıklık **1.40**

> Bu değerleri `app_duckdb.py` başında değiştirebilirsiniz.

---

## 9) Sık karşılaşılan hatalar & çözümler

* **“Node cache yok: …”**
  → Yol yanlış olabilir. Önce kontrol edin:

  ```powershell
  pwd; ls .\cache
  $nodes = (Resolve-Path .\cache\be_poi.parquet).Path
  ```

  Sonra komutta **\$nodes** kullanın.

* **`brand not found in FROM clause` (DuckDB)**
  → Eski `app_duckdb.py` kullanılıyor. Güncel dosyada node tarafı `NULL AS brand` ile seçilir.
  `Select-String -Path .\app_duckdb.py -Pattern "NULL AS brand"` ile doğrulayın.

* **`osmium` kurulamadı**
  → Python 3.11 venv kullanın veya OS’te uygun wheel yükleyin.

* **Cache çok yavaş oluşuyor**
  → Normaldir (tek seferlik). SSD kullanın; antivirüs real-time taramasını bu klasör için geçici kapatabilirsiniz.

* **Harita açılmıyor**
  → Komuttan sonra `map.html` üretildiyse:

  ```powershell
  start .\map.html
  ```

---

## 10) Güncelleme & tekrar cache

OSM verisini yenilemek isterseniz:

1. `data/belgium-latest.osm.pbf` dosyasını güncelleyin.
2. **İki** builder’ı yeniden çalıştırın:

   ```powershell
   python .\build_poi_cache.py --pbf ".\data\belgium-latest.osm.pbf" --out ".\cache\be_poi.parquet"
   python .\build_poi_poly_cache_osmium.py --pbf ".\data\belgium-latest.osm.pbf" --out ".\cache\be_poi_poly.parquet"
   ```

---

## 11) Kategori/Skor mantığını özelleştirme

* Kategoriler ve marker renkleri: `app_duckdb.py` içindeki `CATS` sözlüğü.
* Sıralama ağırlıkları: `SCORES` sözlüğü (SQL CASE ifadeleri).
* POI etiket kapsamı: builder’larda (`build_poi_cache.py` & `build_poi_poly_cache_osmium.py`) `amenity/shop/healthcare/...` kümelerini genişletebilirsiniz.

  > Örn. marketler için `shop=department_store` eklemek gibi.

---

## 12) Minimum hızlı kurulum (özet)

```powershell
# 1) venv
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install duckdb pyarrow pandas folium geopy osmium shapely

# 2) PBF -> data/
# belgium-latest.osm.pbf dosyasını .\data içine koy

# 3) Cache (tek sefer)
python .\build_poi_cache.py --pbf ".\data\belgium-latest.osm.pbf" --out ".\cache\be_poi.parquet"
python .\build_poi_poly_cache_osmium.py --pbf ".\data\belgium-latest.osm.pbf" --out ".\cache\be_poi_poly.parquet"

# 4) Analiz + Harita
$nodes = (Resolve-Path .\cache\be_poi.parquet).Path
$polys = (Resolve-Path .\cache\be_poi_poly.parquet).Path
python .\app_duckdb.py --address "Tervuursesteenweg 147, 3001 Heverlee, Belgium" --radius 2500 --nodes "$nodes" --polys "$polys"
start .\map.html
```

