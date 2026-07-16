# Frontend Tasarım Brief'i — "Ev Çevresi Analizi" (Claude Design'a verilecek prompt)

> Bu belgenin tamamını Claude Design'a (veya tasarım üretecek herhangi bir AI aracına) tek prompt olarak yapıştırabilirsiniz.

---

## Ürün Özeti

**Ev Çevresi Analizi**, Belçika'da ev arayan kişilerin bir adresin "yaşanabilirliğini" saniyeler içinde değerlendirmesini sağlayan bir web uygulamasıdır. Kullanıcı bir adres girer; uygulama OpenStreetMap verisiyle çevredeki **okul, market, sağlık, ulaşım, park ve spor** noktalarını bulur, her kategori için **0–10 arası puan** ve genel bir **yaşanabilirlik puanı** hesaplar, sonuçları interaktif bir haritada gösterir.

Ürünün ruhu: **emlak + konum zekâsı**. Kullanıcıya "bu evi almalı mıyım / kiralamalı mıyım?" sorusunda güven veren, veriye dayalı, sakin ve profesyonel bir arayüz.

## Hedef Kullanıcı

- Belçika'da ev arayan bireyler ve aileler (teknik olmayan kullanıcılar).
- İkincil: emlak danışmanları (birden fazla adresi hızlıca değerlendirmek isterler).
- Arayüz dili: **Türkçe** (metinler Türkçe olacak; ileride çoklu dil düşünülebilir).

## Ekranlar

### 1. Karşılama / Arama Ekranı
- Büyük, odakta bir **adres arama kutusu** (ör. "Tervuursesteenweg 147, 3001 Heverlee").
- Alternatif giriş: **koordinat (enlem/boylam)** — gelişmiş seçenek olarak gizlenebilir.
- **Yarıçap seçici**: 500 m – 5000 m arası slider veya hazır seçenekler (1 km / 2.5 km / 5 km), varsayılan 2.5 km.
- Kısa bir değer önerisi cümlesi + ürünün ne yaptığını anlatan 3 maddelik mini açıklama (okul-market-ulaşım ikonlarıyla).
- Son aramalar (localStorage) — küçük çipler halinde.

### 2. Analiz Sonucu Ekranı (ana ekran)
Yerleşim: **solda harita (ekranın ~%60'ı), sağda skor paneli** (mobilde harita üstte, panel altta).

**Harita (Leaflet):**
- Merkezde ev ikonu ile aranan adres.
- Kategoriye göre renkli marker'lar (renk kodları aşağıda).
- Marker popup: POI adı, yürüme mesafe+süre, araçla mesafe+süre.
- Harita üzerinde yarıçap dairesi (yarı saydam).
- Kategori filtreleme: harita üstünde toggle'lanabilir kategori çipleri (kapatınca o kategorinin marker'ları gizlenir).

**Skor paneli:**
- En üstte **Genel Puan**: büyük, belirgin bir gösterge (0–10). Dairesel gauge veya büyük sayı + renk bandı. Puana göre renk: 0–4 kırmızı, 4–7 amber, 7–10 yeşil. Altında tek cümlelik yorum (ör. "Günlük yaşam için çok elverişli konum").
- Altında **6 kategori kartı** (Okul, Market, Sağlık, Ulaşım, Park, Spor): her kartta kategori ikonu, puan (x.x/10), mini progress bar, POI sayısı ve en yakın mesafe ("en yakın: 350 m").
- Kategori kartına tıklayınca **açılır liste**: o kategorinin TOP-5 POI'si — ad, yürüme (mesafe, süre), araçla (mesafe, süre). Satıra hover'da haritadaki marker vurgulanır.
- Sağlık kategorisinde "hastane var/yok" rozetleri.
- "Raporu paylaş / yazdır" ikincil butonu.

**Yükleme durumu:** analiz 2–10 sn sürebilir. İskelet (skeleton) kartlar + haritada yumuşak bir bekleme animasyonu + "Çevre analiz ediliyor…" mesajı.

**Hata durumları:** adres bulunamadı (öneri: adresi posta koduyla yazın), sunucu hatası, sonuç bulunamadı (yarıçapı büyütme önerisi + tek tık buton).

### 3. (Opsiyonel, v2) Karşılaştırma Ekranı
- 2–3 adresi yan yana kolonlar halinde karşılaştırma: genel puan + kategori puanları tablo/radar grafiği.
- Tasarımda buna yer bırakın ama v1 kapsamında detaylandırmayın.

## Kategori Renkleri ve İkonlar

| Kategori | Anahtar | Renk önerisi | İkon |
|---|---|---|---|
| Okul | `school` | mavi (#2E6FDB) | mezuniyet şapkası / okul |
| Market | `market` | turuncu (#E8842C) | alışveriş sepeti |
| Sağlık | `health` | kırmızı (#D64545) | artı işareti / kalp |
| Ulaşım | `transit` | mor (#7C4DBE) | otobüs / tren |
| Park | `park` | yeşil (#3E9B4F) | ağaç / yaprak |
| Spor | `sport` | camgöbeği (#3A8FA3) | dambıl / koşucu |

Renkleri tasarımın genel paletiyle uyumlu hale getirebilirsiniz; önemli olan 6 kategorinin haritada ve panelde **tutarlı ve ayırt edilebilir** olması (renk körlüğü kontrastına dikkat).

## Çoklu Dil (i18n)

Uygulama **üç dilde** sunulacak: **Türkçe (tr), İngilizce (en), Felemenkçe (nl)**.

- Header'da (sağ üstte) kompakt bir **dil seçici**: ISO kodlu dropdown (TR / EN / NL) veya segment kontrol. Bayrak yerine dil kodu tercih edin (nl için bayrak tartışmalıdır).
- Varsayılan dil tarayıcı dilinden gelir (`navigator.language`); kullanıcının seçimi localStorage'da saklanır ve sonraki ziyaretlerde uygulanır.
- **Tüm** arayüz metinleri çevrilir: butonlar, placeholder'lar, kategori adları, puan yorum cümleleri, yükleme/hata mesajları, dipnotlar.
- Kategori etiketleri ve API mesajları backend'den seçilen dilde gelir (istekte `lang` alanı); statik UI metinleri frontend çeviri sözlüğünden gelir.
- Tasarımda metin genişlemesine yer bırakın: Felemenkçe etiketler uzundur (ör. "Openbaar vervoer" vs "Ulaşım") — kategori kartları ve çipler taşmadan sığmalı.
- Sayı/birim biçimleri her dilde aynı kalır (metrik: m, km, dk/min).

Kategori adları üç dilde:

| key | tr | en | nl |
|---|---|---|---|
| school | Okul | School | School |
| market | Market | Grocery | Supermarkt |
| health | Sağlık | Health | Gezondheid |
| transit | Ulaşım | Transit | Openbaar vervoer |
| park | Park | Park | Park |
| sport | Spor | Sports | Sport |

## Görsel Dil

- Modern, ferah, güven veren; emlak/fintech ciddiyetinde ama sıcak. Referans his: Zillow / Walk Score / Immoweb'in temiz halleri.
- Bol beyaz alan, yumuşak köşeli kartlar (radius 12–16 px), hafif gölgeler.
- Tipografi: okunaklı bir sans-serif (Inter / Manrope gibi); puanlar için belirgin, kalın rakamlar.
- **Açık ve koyu tema** ikisi de tasarlanmalı.
- Tam **responsive**: masaüstü (2 kolon), tablet, mobil (dikey akış; harita yüksekliği kısılır, panel bottom-sheet gibi davranabilir).
- Erişilebilirlik: WCAG AA kontrast, puanlar yalnızca renkle değil sayıyla da ifade edilir.

## Teknik Kısıtlar (tasarımı etkileyenler)

- Harita **Leaflet + OpenStreetMap karoları** ile çizilecek (Google Maps değil). Tasarımda harita alanını buna göre düşünün.
- Frontend tek sayfalık uygulama olacak (vanilla JS veya React); backend Flask JSON API.
- Grafik kütüphanesi olarak hafif bir çözüm tercih edilir (gauge/progress CSS ile çizilebilir).

## API Veri Sözleşmesi (tasarımın bağlanacağı veri)

`POST /api/analyze` → yanıt:

```json
{
  "display_address": "Tervuursesteenweg 147, 3001 Heverlee, Vlaams-Brabant, België",
  "lat": 50.876182,
  "lon": 4.680335,
  "radius": 2500,
  "overall": 7.4,
  "categories": [
    {
      "key": "market",
      "label": "Market",
      "score": 8.2,
      "count": 12,
      "nearest_m": 350,
      "has_hospital": null,
      "items": [
        {
          "name": "Colruyt Heverlee",
          "brand": "Colruyt",
          "lat": 50.8771,
          "lon": 4.6822,
          "walk_m": 440,
          "walk_min": 6,
          "drive_m": 490,
          "drive_min": 1
        }
      ]
    }
  ]
}
```

(`has_hospital` yalnızca `health` kategorisinde `true/false`, diğerlerinde `null`.)

## Teslim Edilecekler

1. Karşılama/arama ekranı (masaüstü + mobil).
2. Analiz sonucu ekranı (masaüstü + mobil), yükleme ve hata durumları dahil.
3. Kategori kartı bileşeninin kapalı/açık halleri.
4. Açık ve koyu tema varyantları.
5. Renk paleti, tipografi ölçeği ve bileşen tanımlarını içeren kısa bir stil rehberi.
