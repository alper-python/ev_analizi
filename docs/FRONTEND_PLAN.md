# Frontend Yol Haritası ve Yapılacaklar

Hedef: `app_duckdb.py`'deki analiz motorunu bir Flask JSON API arkasına alıp, Claude Design ile üretilecek tasarıma göre tek sayfalık bir web arayüzü geliştirmek.

## Mimari Kararlar

- **Backend:** Flask, tek endpoint ağırlıklı (`POST /api/analyze`). Harita HTML'i backend'de üretilmeyecek; backend **ham JSON** (POI listeleri + puanlar) döndürecek, haritayı frontend Leaflet ile çizecek. (`analyze()` fonksiyonundaki `map_html` yaklaşımı terk edilecek — frontend'e esneklik ve daha küçük yanıt.)
- **Frontend:** Framework'süz (vanilla JS + Leaflet) başlamak yeterli; tek ekran ağırlıklı bir uygulama. İstenirse sonradan React'e taşınır.
- **Geocoding:** Nominatim backend'de kalır (rate limit ve user-agent kontrolü tek yerden).

## Aşama 0 — Backend Hazırlık (ön koşul) — ✅ TAMAMLANDI (2026-07-16)

- [x] `main()` ve `analyze()` içindeki ortak mantığı tek çekirdek fonksiyona çıkar (`run_analysis(lat, lon, radius, topn) -> dict`); CLI ve API bunu çağırsın.
- [x] **Node + polygon çift sayım düzeltmesi:** sorguya isim + yuvarlanmış koordinat bazlı dedup eklendi (isimli POI: isim + ~110 m hücre; isimsiz: ~11 m hücre; polygon kaydı tercih edilir).
- [x] **Transit çift sayımı:** `public_transport=stop_position` kayıtları (başka niteleyici etiketi yoksa) sorguda eleniyor.
- [x] `landuse=grass`'ı park kategorisinden çıkar (sorgu filtresi; cache yeniden üretmeye gerek yok).
- [x] SQL'de kullanıcı girdileri DuckDB parametreli sorguya taşındı; parquet yolları kaçışlanıyor.
- [x] Genel puan hesabında `overall / total_weight` normalizasyonu düzeltildi.
- [x] `requirements.txt` gerçek bağımlılıklara indirildi (flask-cors gerekmedi; frontend aynı Flask'tan servis ediliyor).

## Aşama 1 — API — ✅ TAMAMLANDI (2026-07-16)

- [x] `belgium-location/server.py`: Flask uygulaması.
  - [x] `POST /api/analyze` — gövde: `{address?, lat?, lon?, radius=2500, topn=5}`; yanıt: `docs/FRONTEND_DESIGN_BRIEF.md`'deki veri sözleşmesi.
  - [x] Hata yanıtları: 400 (adres çözülemedi / parametre hatalı), 502 (geocode servisi), 503 (cache yok). Sonuçsuz bölge: 200 + `note` alanı (yarıçap büyütme önerisi).
  - [x] Nominatim çağrısına timeout (10 sn) + anlamlı hata mesajı.
  - [x] Basit in-memory cache: aynı (lat,lon,radius,topn) için 1 saat TTL.
- [x] `GET /api/health` — cache dosyalarının varlığını raporluyor.
- [x] Statik dosya servis etme (`/` → `static/index.html`; şimdilik geçici API test sayfası, tasarım gelince değişecek).

> Doğrulama: sentetik parquet cache'lerle 17 kontrol içeren test (dedup, stop_position/grass filtreleri, tırnaklı yol, API sözleşmesi, hata durumları) + CLI'nin map.html üretimi — tümü geçti.

## Aşama 2 — Tasarım — ✅ TAMAMLANDI (2026-07-16)

- [x] `docs/FRONTEND_DESIGN_BRIEF.md` içeriğini Claude Design'a ver, tasarımı ürettir.
- [x] Çıkan tasarım alındı: `Ev Çevresi Analizi.dc.html` + `support.js` (Claude Design prototip formatı — `x-dc`/`DCLogic`).
- [x] Tasarım incelendi: 3 dil (TR/EN/NL) gömülü, 6 kategori renk/ikon, açık+koyu tema, responsive, yükleme/hata durumları, stil rehberi ekranı hepsi mevcut.

## Aşama 3 — Frontend Geliştirme — ✅ BÜYÜK ÖLÇÜDE TAMAMLANDI (2026-07-16)

Tasarım Claude Design prototip formatında geldi; `support.js` React 18 + Babel'i CDN'den yükleyip DC bileşenini otomatik boot ediyor → Flask'tan doğrudan servis edilebiliyor. Ayrı `app.js`/`styles.css` yazmak yerine tasarım olduğu gibi kullanıldı, yalnızca veri katmanı gerçek API'ye bağlandı.

- [x] `belgium-location/static/` altına tasarım kopyalandı: `index.html` (tasarım) + `support.js` (DC runtime). Eski geçici test sayfası bununla değiştirildi.
- [x] **Mock veri → gerçek API:** `doSearch()` içindeki sahte `buildData()`/`setTimeout` kaldırıldı; yerine `POST /api/analyze` (fetch). "enlem, boylam" biçimi otomatik algılanıp koordinat olarak, aksi halde adres olarak gönderiliyor. Yarış koşulu (stale request) korumalı.
- [x] Hata eşlemesi: HTTP 400 → "adres bulunamadı", diğer/ağ hatası → "sunucu hatası", tüm kategoriler boş → "sonuç yok" (yarıçap büyütme butonu ile).
- [x] Karşılama ekranı, sonuç ekranı (harita + skor paneli + kategori kartları + filtre çipleri), yükleme (skeleton), hata durumları, açık/koyu tema, responsive, dil seçici — tasarımda hazır ve API'ye bağlı.

> Doğrulama: Flask test client ile `/`, `/support.js` ve `/api/analyze` servis testi (8 kontrol) geçti. **Tarayıcı render'ı gerçek cache dosyaları + internet (CDN) ile canlı test edilmeli** — aşağıya bakınız.

## Aşama 4 — Cila ve Doğrulama (SIRADA)

- [ ] **Canlı tarayıcı testi (ÖNCELİK):** gerçek `be_poi.parquet` + `be_poi_poly.parquet` ile `python server.py` çalıştır, tarayıcıda `http://127.0.0.1:5000` aç; adresle arama → harita + puanlar, dil değişimi, koyu tema, mobil görünüm ve hata durumlarını gözle doğrula.
- [x] Yazdır/paylaş görünümü — tasarımda `window.print` butonu mevcut. *(basit CSS iyileştirmesi opsiyonel)*
- [ ] README'ye web arayüzü bölümü ekle (kurulum + `python server.py`).
- [x] `map.html`'i git'ten çıkar, `.gitignore`'a ekle. *(2026-07-16'da yapıldı)*
- [ ] (Opsiyonel) `support.js`'in CDN bağımlılıkları (React/Babel/Leaflet unpkg'den) — internetsiz/kurumsal ortamda çalışması gerekiyorsa bunları yerelde barındırmayı değerlendir.
- [ ] (Opsiyonel v2) Adres karşılaştırma ekranı (tasarımda yer tutucu "yakında" kartı var).

## İleride Değerlendirilecek — Canlı Veri (Hibrit Overpass Modu)

> Not olarak eklendi (2026-07-16). Şu an veri, PBF'ten üretilen parquet cache'ten (anlık görüntü) geliyor; güncel tutmak için periyodik yeniden üretim gerekiyor. İleride tazelik istenirse hibrit bir mod eklenebilir.

- **Fikir:** Cache'i hız için tut (tüm ülke, çok sorgu), ama istenirse tek adres analizinde **Overpass API**'den canlı POI çek → her zaman güncel veri.
- **Yaklaşım:** `server.py`'ye opsiyonel bir kaynak seçici (`source=cache|live`) veya frontend'de "Canlı veri (güncel)" toggle'ı; `live` seçildiğinde `run_analysis` yerine Overpass sorgusu (nokta + yarıçap) → aynı puanlama/veri sözleşmesine dönüştür.
- **Overpass notları:** Ücretsiz public sunucular (overpass-api.de, kumi.systems); hız limiti ve ara sıra kesinti var → timeout + cache fallback şart. Sorgu, mevcut kategori etiket kümeleriyle (`amenity/shop/healthcare/...`) hizalanmalı.
- **Alternatifler:** Ticari Places API'leri (Google Places, Foursquare, HERE, Geoapify) — daha zengin/güncel veri + SLA, ama istek başına ücret. Yüksek veri kalitesi gerekiyorsa değerlendirilir.
- **Basit alternatif (kod gerektirmez):** Cache'i güncel tutmak için PBF'i periyodik (haftalık/aylık) yeniden indirip iki builder'ı çalıştırmak; Geofabrik günlük güncelleniyor.

## Riskler / Notlar

- **CDN bağımlılığı:** Frontend, tarayıcıda React 18 + Babel'i (support.js üzerinden unpkg.com), Manrope fontunu (Google Fonts), Leaflet'i (unpkg) ve harita karolarını (cartocdn) internetten çeker. İlk render için internet şart; internetsiz ortamda çalışmaz (yukarıdaki opsiyonel maddeye bakınız).
- Nominatim kullanım politikası: saniyede 1 istek; API'de rate-limit ve cache bunu karşılıyor.
- Parquet cache dosyaları repo dışında; cache yoksa API 503 + anlamlı mesaj döner.
- Mesafe/süre değerleri kuş uçuşu × dolaşıklık tahminidir; UI'da "≈"/dipnot ile belirtmek dürüst olur. Gerçek rota süreleri (OSRM) v2 konusu.
