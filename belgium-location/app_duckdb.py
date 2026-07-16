# app_duckdb.py — node + polygon cache ile hızlı POI analizi + puanlama
#
# Çekirdek: run_analysis(lat, lon, ...) → ham analiz verisi (dict).
# CLI (main) bu veriden konsol çıktısı + folium haritası (map.html) üretir.
# Web API (server.py) aynı çekirdeği çağırıp JSON döndürür.
import os, math, argparse
import duckdb, pandas as pd, folium
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# ========== KULLANICI AYARLANABİLİR PARAMETRELER ==========

TOP_N = 5
DEFAULT_RADIUS_M = 2500
DEFAULT_NODES = "./cache/be_poi.parquet"
DEFAULT_POLYS = "./cache/be_poi_poly.parquet"

# Yürüyüş/araç yaklaşımı (yaklaşık, yol dolaşıklığı dahil)
WALK_SPEED_KPH, DRIVE_SPEED_KPH = 4.8, 35.0
WALK_CIRCUITY, DRIVE_CIRCUITY = 1.25, 1.40

# Desteklenen diller (API: lang parametresi; CLI Türkçe çalışır)
SUPPORTED_LANGS = ("tr", "en", "nl")
DEFAULT_LANG = "tr"

# Kategori isimleri, etiketleri (tr/en/nl) ve renkleri
CATS = {
    "school": {"labels": {"tr": "Okul",   "en": "School",  "nl": "School"},           "color": "blue"},
    "market": {"labels": {"tr": "Market", "en": "Grocery", "nl": "Supermarkt"},       "color": "orange"},
    "health": {"labels": {"tr": "Sağlık", "en": "Health",  "nl": "Gezondheid"},       "color": "red"},
    "transit":{"labels": {"tr": "Ulaşım", "en": "Transit", "nl": "Openbaar vervoer"}, "color": "purple"},
    "park":   {"labels": {"tr": "Park",   "en": "Park",    "nl": "Park"},             "color": "green"},
    "sport":  {"labels": {"tr": "Spor",   "en": "Sports",  "nl": "Sport"},            "color": "cadetblue"},
}

def cat_label(cat, lang=DEFAULT_LANG):
    labels = CATS[cat]["labels"]
    return labels.get(lang, labels[DEFAULT_LANG])

# Genel puan ağırlıkları (toplamı 1.0 olmak zorunda değil; normalize edilir)
OVERALL_WEIGHTS = {
    "market": 0.25,
    "school": 0.25,
    "health": 0.20,
    "transit":0.15,
    "park":   0.10,
    "sport":  0.05,
}

# Kategori içi puanlama konfigürasyonu:
# D0: yakınlık doygunluk mesafesi (m)
# w_prox + w_count = 10 (puanların dağılımı)
# Nsat: sayıda doygunluğa ulaşılacak değer
# bonus: opsiyonel; health için hastane varsa +1 gibi
SCORING = {
    "market": {"D0": 1500, "w_prox": 6.0, "w_count": 4.0, "Nsat": 3},
    "school": {"D0": 2000, "w_prox": 5.0, "w_count": 5.0, "Nsat": 4},
    "health": {"D0": 4000, "w_prox": 7.0, "w_count": 3.0, "Nsat": 3, "bonus_if_hospital": 1.0},
    "transit":{"D0": 800,  "w_prox": 7.0, "w_count": 3.0, "Nsat": 5},
    "park":   {"D0": 1200, "w_prox": 6.0, "w_count": 4.0, "Nsat": 3},
    "sport":  {"D0": 1500, "w_prox": 5.0, "w_count": 5.0, "Nsat": 3},
}

# ===========================================================

def fmt_meters(m):
    return "" if m is None else (f"{m/1000:.2f} km" if m>=1000 else f"{int(round(m))} m")

def fmt_seconds(s):
    if s is None: return ""
    m = int(round(s/60))
    return f"{m} dk" if m<60 else (f"{m//60} sa {m%60} dk" if m%60 else f"{m//60} sa")

def meters_to_deg_latlon(lat, r_m):
    dlat = r_m/111320.0
    dlon = r_m/(111320.0*max(0.1, math.cos(math.radians(lat))))
    return dlat, dlon

def geocode(address):
    geocoder = Nominatim(user_agent="be-poi-cache/1.3", timeout=10)
    rl = RateLimiter(geocoder.geocode, min_delay_seconds=1.0)
    loc = rl(address)
    if not loc: raise RuntimeError("Adres geocode edilemedi.")
    return loc.latitude, loc.longitude, loc.address

# Alt-skor (tür ağırlıkları) — kategori içi sıralama için; puanlamadan bağımsız
SCORES = {
    "school": """
        (CASE WHEN amenity='school' THEN 10 ELSE 0 END) +
        (CASE WHEN amenity='college' THEN 7 ELSE 0 END) +
        (CASE WHEN amenity='kindergarten' THEN 6 ELSE 0 END) +
        (CASE WHEN "school_level" IS NOT NULL THEN 4 ELSE 0 END) +
        (CASE WHEN "isced_level" IS NOT NULL THEN 4 ELSE 0 END)
    """,
    "market": """
        (CASE WHEN shop='supermarket' THEN 10 ELSE 0 END) +
        (CASE WHEN shop='convenience' THEN 8 ELSE 0 END) +
        (CASE WHEN amenity='marketplace' THEN 6 ELSE 0 END)
    """,
    "health": """
        (CASE WHEN amenity='hospital' OR healthcare='hospital' THEN 100 ELSE 0 END) +
        (CASE WHEN amenity='clinic' OR healthcare='clinic' THEN 80 ELSE 0 END) +
        (CASE WHEN amenity='doctors' OR healthcare='doctor' THEN 60 ELSE 0 END) +
        (CASE WHEN amenity='dentist' OR healthcare='dentist' THEN 55 ELSE 0 END) +
        (CASE WHEN healthcare='physiotherapist' THEN 50 ELSE 0 END) +
        (CASE WHEN amenity='pharmacy' THEN 40 ELSE 0 END) +
        (CASE WHEN healthcare IS NOT NULL THEN 30 ELSE 0 END)
    """,
    "transit": """
        (CASE WHEN railway='station' THEN 100 ELSE 0 END) +
        (CASE WHEN railway='halt' THEN 90 ELSE 0 END) +
        (CASE WHEN amenity='bus_station' THEN 80 ELSE 0 END) +
        (CASE WHEN railway='tram_stop' OR railway='subway_entrance' THEN 70 ELSE 0 END) +
        (CASE WHEN highway='bus_stop' THEN 50 ELSE 0 END) +
        (CASE WHEN public_transport IS NOT NULL THEN 40 ELSE 0 END)
    """,
    "park": """
        (CASE WHEN leisure='park' THEN 100 ELSE 0 END) +
        (CASE WHEN leisure='garden' THEN 80 ELSE 0 END) +
        (CASE WHEN leisure='nature_reserve' OR boundary='national_park' THEN 80 ELSE 0 END) +
        (CASE WHEN leisure='recreation_ground' THEN 60 ELSE 0 END) +
        (CASE WHEN leisure='playground' THEN 50 ELSE 0 END) +
        (CASE WHEN boundary='national_park' THEN 15 ELSE 0 END)
    """,
    "sport": """
        (CASE WHEN leisure='fitness_centre' OR amenity='gym' THEN 90 ELSE 0 END) +
        (CASE WHEN leisure='sports_centre' THEN 80 ELSE 0 END) +
        (CASE WHEN sport IS NOT NULL THEN 20 ELSE 0 END)
    """,
}

# Kategoriye özel ek filtreler (cache'i yeniden üretmeye gerek kalmadan sorguda uygulanır):
# - transit: public_transport=stop_position, platform'un teknik kopyasıdır;
#   başka niteleyici etiketi yoksa sayılmaz (çift sayım puanı şişiriyordu).
# - park: yalnızca landuse=grass ile eşleşen çim parçaları park sayılmaz.
CAT_FILTERS = {
    "transit": "(public_transport IS DISTINCT FROM 'stop_position' OR railway IS NOT NULL OR highway = 'bus_stop' OR amenity = 'bus_station')",
    "park": "NOT (landuse = 'grass' AND leisure IS NULL AND boundary IS NULL)",
}

COMMON_COLS = ("lat, lon, amenity, shop, healthcare, railway, highway, "
               "public_transport, leisure, boundary, landuse, sport, school_level, isced_level")

def query_category(con, nodes_path, polys_path, cat, lat, lon, radius_m, topn):
    dlat, dlon = meters_to_deg_latlon(lat, radius_m)
    score_sql = SCORES[cat]
    extra = CAT_FILTERS.get(cat)
    extra_sql = f"AND {extra}" if extra else ""

    # Parquet yolları sunucu/CLI konfigürasyonundan gelir; SQL'e gömmeden önce
    # tek tırnaklar kaçışlanır. Kullanıcı girdileri ($cat, $lat, ...) parametredir.
    parts = []
    if nodes_path:
        p = str(nodes_path).replace("'", "''")
        # node cache'de brand kolonu yok → NULL AS brand; dedup'ta polygon tercih edilir (src_pref)
        parts.append(
            f"SELECT name, NULL AS brand, {COMMON_COLS}, 1 AS src_pref "
            f"FROM read_parquet('{p}') WHERE cat = $cat"
        )
    if polys_path:
        p = str(polys_path).replace("'", "''")
        parts.append(
            f"SELECT name, brand, {COMMON_COLS}, 0 AS src_pref "
            f"FROM read_parquet('{p}') WHERE cat = $cat"
        )
    if not parts:
        return pd.DataFrame()

    base_src = " UNION ALL ".join(parts)

    # dedup: aynı POI hem node hem polygon olarak etiketlenmiş olabilir.
    # İsimli POI'ler isim + ~110 m hücre (round 3) ile, isimsizler ~11 m hücre (round 4)
    # ile tekilleştirilir; polygon kaydı (brand içerir) tercih edilir.
    # Not: çok büyük alanlarda (centroid girişten 110 m'den uzaksa) kalıntı kopya kalabilir.
    q = f"""
    WITH base AS (
      SELECT * FROM ({base_src})
      WHERE lat BETWEEN $lat_min AND $lat_max
        AND lon BETWEEN $lon_min AND $lon_max
        {extra_sql}
    ),
    dedup AS (
      SELECT * FROM (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY
              CASE WHEN name IS NOT NULL THEN lower(name)
                   ELSE cast(round(lat,4) AS VARCHAR) || ',' || cast(round(lon,4) AS VARCHAR) END,
              CASE WHEN name IS NOT NULL THEN round(lat,3) ELSE 0.0 END,
              CASE WHEN name IS NOT NULL THEN round(lon,3) ELSE 0.0 END
            ORDER BY src_pref
          ) AS rn_dupe
        FROM base
      ) WHERE rn_dupe = 1
    ),
    dist AS (
      SELECT *,
        2*6371000*asin(
          sqrt(
            sin(radians(lat - $lat)/2)*sin(radians(lat - $lat)/2) +
            cos(radians($lat))*cos(radians(lat))*
            sin(radians(lon - $lon)/2)*sin(radians(lon - $lon)/2)
          )
        ) AS d_lin
      FROM dedup
    ),
    scored AS (
      SELECT *,
        {score_sql} AS score,
        (CASE WHEN amenity='hospital' OR healthcare='hospital' THEN 1 ELSE 0 END) AS is_hospital
      FROM dist
      WHERE d_lin <= $radius
    ),
    ranked AS (
      SELECT *,
        COUNT(*) OVER () AS n_total,
        MIN(d_lin) OVER () AS d_min,
        MAX(is_hospital) OVER () AS has_hospital_any,
        d_lin*{WALK_CIRCUITY} AS walk_m,
        d_lin*{DRIVE_CIRCUITY} AS drive_m,
        (d_lin*{WALK_CIRCUITY}) / ({WALK_SPEED_KPH} * 1000/3600) AS walk_s,
        (d_lin*{DRIVE_CIRCUITY}) / ({DRIVE_SPEED_KPH} * 1000/3600) AS drive_s,
        ROW_NUMBER() OVER (ORDER BY score DESC, d_lin ASC) AS rn_all
      FROM scored
    )
    SELECT name, brand, amenity, shop, healthcare,
           lat, lon, score, d_lin, d_min, n_total, has_hospital_any,
           walk_m, walk_s, drive_m, drive_s
    FROM ranked
    WHERE rn_all <= $topn
    ORDER BY rn_all
    """
    params = {
        "cat": cat, "lat": lat, "lon": lon, "radius": radius_m, "topn": topn,
        "lat_min": lat - dlat, "lat_max": lat + dlat,
        "lon_min": lon - dlon, "lon_max": lon + dlon,
    }
    return con.execute(q, params).df()

def calc_category_score(cat, n_total:int, d_min:float, has_hospital:bool=False) -> float:
    cfg = SCORING[cat]
    D0 = cfg["D0"]
    w_prox = cfg["w_prox"]
    w_count = cfg["w_count"]
    Nsat = cfg["Nsat"]

    # yakınlık puanı (0..w_prox)
    if d_min is None or n_total == 0:
        prox_pts = 0.0
    else:
        prox_norm = max(0.0, 1.0 - min(d_min, D0)/D0)  # 0..1
        prox_pts = prox_norm * w_prox

    # sayıya bağlı puan (0..w_count)
    count_norm = min(n_total, Nsat) / Nsat  # 0..1
    count_pts = count_norm * w_count

    score = prox_pts + count_pts

    # sağlıkta hastane bonusu (varsa)
    if cat == "health" and has_hospital and "bonus_if_hospital" in cfg:
        score += cfg["bonus_if_hospital"]

    return max(0.0, min(10.0, score))

def _cell(v):
    return None if pd.isnull(v) else v

def run_analysis(lat, lon, radius=DEFAULT_RADIUS_M, topn=TOP_N,
                 nodes_path=DEFAULT_NODES, polys_path=DEFAULT_POLYS, lang=DEFAULT_LANG):
    """Analiz çekirdeği: ham sayısal sonuç döndürür (CLI ve API bunun üzerine kurulur).
    lang yalnızca kategori etiketlerini etkiler; sayısal sonuç dilden bağımsızdır."""
    nodes_ok = bool(nodes_path) and os.path.exists(nodes_path)
    polys_ok = bool(polys_path) and os.path.exists(polys_path)
    if not nodes_ok and not polys_ok:
        raise FileNotFoundError(
            f"Ne node ne polygon cache bulundu.\n  nodes: {nodes_path}\n  polys: {polys_path}\n"
            f"Lütfen cache dosyalarını üretin (build_poi_cache.py / build_poi_poly_cache_osmium.py).")

    con = duckdb.connect()
    categories = []
    cat_scores = {}

    for cat, meta in CATS.items():
        df = query_category(con, nodes_path if nodes_ok else None,
                            polys_path if polys_ok else None,
                            cat, lat, lon, radius, topn)
        if df.empty:
            score, n_total, d_min, has_hospital, items = 0.0, 0, None, False, []
        else:
            n_total = int(df.iloc[0]["n_total"])
            d_min = float(df.iloc[0]["d_min"]) if pd.notnull(df.iloc[0]["d_min"]) else None
            has_hospital = bool(df.iloc[0]["has_hospital_any"])
            score = calc_category_score(cat, n_total, d_min, has_hospital)
            items = [{
                "name": _cell(r["name"]),
                "brand": _cell(r["brand"]),
                "lat": float(r["lat"]), "lon": float(r["lon"]),
                "walk_m": float(r["walk_m"]), "walk_s": float(r["walk_s"]),
                "drive_m": float(r["drive_m"]), "drive_s": float(r["drive_s"]),
            } for _, r in df.iterrows()]

        cat_scores[cat] = score
        categories.append({
            "key": cat, "label": cat_label(cat, lang), "color": meta["color"],
            "score": round(score, 1),
            "count": n_total,
            "nearest_m": int(round(d_min)) if d_min is not None else None,
            "has_hospital": has_hospital if cat == "health" else None,
            "items": items,
        })

    total_weight = sum(OVERALL_WEIGHTS.get(c, 0.0) for c in CATS)
    overall = (sum(OVERALL_WEIGHTS.get(c, 0.0) * cat_scores[c] for c in CATS) / total_weight
               if total_weight > 0 else 0.0)

    return {
        "lat": lat, "lon": lon, "radius": radius,
        "overall": round(overall, 1),
        "categories": categories,
        "sources": {"nodes": nodes_ok, "polys": polys_ok},
    }

def build_map(result, disp):
    """run_analysis sonucundan folium haritası üretir (CLI çıktısı)."""
    lat, lon = result["lat"], result["lon"]
    m = folium.Map(location=[lat, lon], zoom_start=15, control_scale=True)
    folium.Marker([lat, lon], popup=f"Adres: {disp}", tooltip="Adres",
                  icon=folium.Icon(color="black", icon="home")).add_to(m)
    folium.Circle([lat, lon], radius=result["radius"], color="#666",
                  weight=1, fill=True, fill_opacity=0.05).add_to(m)

    for c in result["categories"]:
        for it in c["items"]:
            popup = (f"{c['label']}: {it['name']}<br>"
                     f"Yürüme: {fmt_meters(it['walk_m'])}, {fmt_seconds(it['walk_s'])}<br>"
                     f"Araba: {fmt_meters(it['drive_m'])}, {fmt_seconds(it['drive_s'])}")
            folium.Marker([it["lat"], it["lon"]],
                          popup=popup, tooltip=f"{c['label']}: {it['name']}",
                          icon=folium.Icon(color=c["color"])).add_to(m)

    # Legend (etiketler run_analysis sonucundan gelir → dil tutarlı)
    entries = "".join(
        f'<div style="display:flex;align-items:center;margin:2px 0;">'
        f'<span style="display:inline-block;width:12px;height:12px;background:{c["color"]};margin-right:6px;border:1px solid #333;"></span>'
        f'{c["label"]}</div>' for c in result["categories"]
    )
    m.get_root().html.add_child(folium.Element(
        f'<div style="position:fixed;bottom:10px;left:10px;z-index:9999;background:#fff;padding:8px 10px;'
        f'border:1px solid #999;border-radius:6px;font-size:13px;">'
        f'<div style="font-weight:600;margin-bottom:4px;">Legenda</div>{entries}</div>'
    ))

    # Scorecard overlay
    score_items = "".join(
        f'<div style="display:flex;justify-content:space-between;"><span>{c["label"]}</span>'
        f'<span>{c["score"]:0.1f}/10</span></div>'
        for c in result["categories"]
    )
    m.get_root().html.add_child(folium.Element(
        f'<div style="position:fixed;top:10px;right:10px;z-index:9999;background:#fff;padding:10px 12px;'
        f'border:1px solid #999;border-radius:6px;font-size:13px;min-width:200px;">'
        f'<div style="font-weight:700;margin-bottom:6px;">Puanlama</div>'
        f'{score_items}'
        f'<hr style="margin:6px 0;border:none;border-top:1px solid #ddd;" />'
        f'<div style="display:flex;justify-content:space-between;font-weight:700;">'
        f'<span>Genel</span><span>{result["overall"]:0.1f}/10</span>'
        f'</div></div>'
    ))
    return m

def main():
    ap = argparse.ArgumentParser(description="Adres çevresinde hızlı POI analizi (node+polygon cache, puanlama).")
    ap.add_argument("--address", type=str)
    ap.add_argument("--lat", type=float)
    ap.add_argument("--lon", type=float)
    ap.add_argument("--radius", type=int, default=DEFAULT_RADIUS_M)
    ap.add_argument("--topn", type=int, default=TOP_N)
    ap.add_argument("--nodes", type=str, default=DEFAULT_NODES)
    ap.add_argument("--polys", type=str, default=DEFAULT_POLYS)
    args = ap.parse_args()

    if args.address:
        lat, lon, disp = geocode(args.address)
    elif args.lat is not None and args.lon is not None:
        lat, lon, disp = args.lat, args.lon, f"({args.lat:.6f}, {args.lon:.6f})"
    else:
        raise SystemExit("Adres veya (lat,lon) verin.")

    print(f"Adres: {disp}  (lat={lat:.6f}, lon={lon:.6f})")
    try:
        result = run_analysis(lat, lon, radius=args.radius, topn=args.topn,
                              nodes_path=args.nodes, polys_path=args.polys)
    except FileNotFoundError as e:
        raise SystemExit(str(e))

    src = result["sources"]
    if src["nodes"] and not src["polys"]:
        print("[INFO] Sadece NODE cache bulunuyor.")
    elif src["polys"] and not src["nodes"]:
        print("[INFO] Sadece POLYGON cache bulunuyor (node yok).")
    else:
        print("[INFO] Node + Polygon birlikte kullanılacak.")

    for c in result["categories"]:
        label = c["label"]
        if not c["items"]:
            print(f"\n— {label} (sonuç yok)")
            continue
        print(f"\n— {label} (TOP {len(c['items'])})")
        for it in c["items"]:
            print(f"{label:<8} | {str(it['name'])[:48]:<48} | "
                  f"Yürüme: {fmt_meters(it['walk_m']):>8}, {fmt_seconds(it['walk_s']):>8} | "
                  f"Araba: {fmt_meters(it['drive_m']):>8}, {fmt_seconds(it['drive_s']):>8}")
        dmin_txt = fmt_meters(c["nearest_m"]) if c["nearest_m"] is not None else "-"
        hosp_txt = ""
        if c["key"] == "health":
            hosp_txt = " (hastane: var)" if c["has_hospital"] else " (hastane: yok)"
        print(f"   ⇒ Puan: {c['score']:.1f}/10  | n={c['count']}  | en yakın={dmin_txt}{hosp_txt}")

    print("\n=== Kategori Puanları ===")
    for c in result["categories"]:
        print(f"{c['label']:<8}: {c['score']:>4.1f}/10")
    print(f"\n*** GENEL PUAN: {result['overall']:.1f}/10 ***")

    m = build_map(result, disp)
    out = os.path.abspath("map.html")
    m.save(out)
    print(f"\nHarita kaydedildi: {out}")

if __name__ == "__main__":
    main()
