# belgium-location

Bu klasör analiz motorunu, web sunucusunu, cache üreticilerini ve frontend'i içerir.

Kurulum, veri/cache üretimi, web arayüzü ve CLI kullanımı için proje kökündeki **[README.md](../README.md)** dosyasına bakın.

Hızlı özet:

```powershell
cd belgium-location
.\.venv\Scripts\Activate.ps1

# Web arayüzü
$env:POI_NODES = (Resolve-Path ..\cache\be_poi.parquet).Path
$env:POI_POLYS = (Resolve-Path ..\cache\be_poi_poly.parquet).Path
python .\server.py            # http://127.0.0.1:5000

# veya CLI (harita üretir)
python .\app_duckdb.py --address "Tervuursesteenweg 147, 3001 Heverlee, Belgium" --radius 2500
```

Dosyalar: `app_duckdb.py` (çekirdek + CLI), `server.py` (web API), `build_poi_cache.py` / `build_poi_poly_cache_osmium.py` (cache üreticileri), `static/` (frontend).
