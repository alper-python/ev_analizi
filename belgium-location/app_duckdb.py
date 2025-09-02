# app_duckdb.py — node cache (varsa) + polygon cache (varsa) ile hızlı POI analizi + PUANLAMA
import os, math, argparse
import duckdb, pandas as pd, folium
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# ========== KULLANICI AYARLANABİLİR PARAMETRELER ==========

TOP_N = 5
DEFAULT_RADIUS_M = 2500

# Yürüyüş/araç yaklaşımı (yaklaşık, yol dolaşıklığı dahil)
WALK_SPEED_KPH, DRIVE_SPEED_KPH = 4.8, 35.0
WALK_CIRCUITY, DRIVE_CIRCUITY = 1.25, 1.40

# Kategori isimleri, etiketleri ve renkleri
CATS = {
    "school": {"label": "Okul",   "color": "blue"},
    "market": {"label": "Market", "color": "orange"},
    "health": {"label": "Sağlık", "color": "red"},
    "transit":{"label": "Ulaşım", "color": "purple"},
    "park":   {"label": "Park",   "color": "green"},
    "sport":  {"label": "Spor",   "color": "cadetblue"},
}

# Genel puan ağırlıkları (toplamı = 1.0)
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
    geocoder = Nominatim(user_agent="be-poi-cache/1.3")
    rl = RateLimiter(geocoder.geocode, min_delay_seconds=1.0)
    loc = rl(address)
    if not loc: raise RuntimeError("Adres geocode edilemedi.")
    return loc.latitude, loc.longitude, loc.address

# Alt-skor (tür ağırlıkları) — sıralama için; puanlamadan bağımsız
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
        (CASE WHEN landuse='grass' THEN 20 ELSE 0 END) +
        (CASE WHEN boundary='national_park' THEN 15 ELSE 0 END)
    """,
    "sport": """
        (CASE WHEN leisure='fitness_centre' OR amenity='gym' THEN 90 ELSE 0 END) +
        (CASE WHEN leisure='sports_centre' THEN 80 ELSE 0 END) +
        (CASE WHEN sport IS NOT NULL THEN 20 ELSE 0 END)
    """,
}

def query_category(con, nodes_path, polys_path, cat, lat, lon, radius_m, topn):
    dlat, dlon = meters_to_deg_latlon(lat, radius_m)
    lat_min, lat_max = lat - dlat, lat + dlat
    lon_min, lon_max = lon - dlon, lon + dlon
    score_sql = SCORES[cat]

    parts = []
    if nodes_path:
        # node cache’de brand kolonu yok → NULL AS brand
        parts.append(
            f"SELECT name, NULL AS brand, lat, lon, amenity, shop, healthcare, railway, highway, public_transport, leisure, boundary, landuse, sport, school_level, isced_level "
            f"FROM read_parquet('{nodes_path}') WHERE cat='{cat}'"
        )
    if polys_path:
        parts.append(
            f"SELECT name, brand, lat, lon, amenity, shop, healthcare, railway, highway, public_transport, leisure, boundary, landuse, sport, school_level, isced_level "
            f"FROM read_parquet('{polys_path}') WHERE cat='{cat}'"
        )
    if not parts:
        return pd.DataFrame()

    base_src = " UNION ALL ".join(parts)

    # not: 'is_hospital' ve window fonksiyonları ile n_total, d_min ve has_hospital_any de getiriyoruz
    q = f"""
    WITH base AS (
      SELECT * FROM ({base_src})
      WHERE lat BETWEEN {lat_min} AND {lat_max}
        AND lon BETWEEN {lon_min} AND {lon_max}
    ),
    dist AS (
      SELECT *,
        2*6371000*asin(
          sqrt(
            sin(radians(lat - {lat})/2)*sin(radians(lat - {lat})/2) +
            cos(radians({lat}))*cos(radians(lat))*
            sin(radians(lon - {lon})/2)*sin(radians(lon - {lon})/2)
          )
        ) AS d_lin
      FROM base
    ),
    scored AS (
      SELECT *,
        {score_sql} AS score,
        (CASE WHEN amenity='hospital' OR healthcare='hospital' THEN 1 ELSE 0 END) AS is_hospital
      FROM dist
      WHERE d_lin <= {radius_m}
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
    WHERE rn_all <= {topn}
    """
    return con.execute(q).df()

def calc_category_score(cat, n_total:int, d_min:float, has_hospital:bool=False) -> float:
    # Kategoriye göre konfig
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

    # 0..10 aralığına kırp
    return max(0.0, min(10.0, score))

def main():
    ap = argparse.ArgumentParser(description="Adres çevresinde hızlı POI analizi (node+polygon cache, puanlama).")
    ap.add_argument("--address", type=str)
    ap.add_argument("--lat", type=float)
    ap.add_argument("--lon", type=float)
    ap.add_argument("--radius", type=int, default=DEFAULT_RADIUS_M)
    ap.add_argument("--topn", type=int, default=TOP_N)
    ap.add_argument("--nodes", type=str, default="./cache/be_poi.parquet")
    ap.add_argument("--polys", type=str, default="./cache/be_poi_poly.parquet")
    args = ap.parse_args()

    # konum
    if args.address:
        lat, lon, disp = geocode(args.address)
    elif args.lat is not None and args.lon is not None:
        lat, lon, disp = args.lat, args.lon, f"({args.lat:.6f}, {args.lon:.6f})"
    else:
        raise SystemExit("Adres veya (lat,lon) verin.")

    # kaynaklar (fallback)
    nodes_path = args.nodes if (args.nodes and os.path.exists(args.nodes)) else None
    polys_path = args.polys if (args.polys and os.path.exists(args.polys)) else None
    if not nodes_path and not polys_path:
        raise SystemExit(f"Ne node ne polygon cache bulundu.\n  nodes arg: {args.nodes}\n  polys arg: {args.polys}\nLütfen cache dosyalarını üretin.")

    print(f"Adres: {disp}  (lat={lat:.6f}, lon={lon:.6f})")
    if nodes_path and not polys_path:
        print("[INFO] Sadece NODE cache bulunuyor.")
    elif polys_path and not nodes_path:
        print("[INFO] Sadece POLYGON cache bulunuyor (node yok).")
    else:
        print("[INFO] Node + Polygon birlikte kullanılacak.")

    con = duckdb.connect()
    all_rows=[]
    cat_scores={}
    summary_rows=[]  # kategori scorecard için

    for cat in CATS.keys():
        df = query_category(con, nodes_path, polys_path, cat, lat, lon, args.radius, args.topn)
        label = CATS[cat]["label"]
        if df.empty:
            print(f"\n— {label} (sonuç yok)")
            cat_scores[cat] = 0.0
            summary_rows.append((label, 0.0, 0, None, False))
            continue

        # Konsola TOP-N
        print(f"\n— {label} (TOP {len(df)})")
        for _, r in df.iterrows():
            print(f"{label:<8} | {str(r['name'])[:48]:<48} | "
                  f"Yürüme: {fmt_meters(r['walk_m']):>8}, {fmt_seconds(r['walk_s']):>8} | "
                  f"Araba: {fmt_meters(r['drive_m']):>8}, {fmt_seconds(r['drive_s']):>8}")

        # Puanlama verileri (CTE window'dan aynı değerler tüm satırlarda aynı)
        n_total = int(df.iloc[0]["n_total"])
        d_min   = float(df.iloc[0]["d_min"]) if pd.notnull(df.iloc[0]["d_min"]) else None
        has_hospital = bool(df.iloc[0]["has_hospital_any"]) if "has_hospital_any" in df.columns else False

        score = calc_category_score(cat, n_total, d_min, has_hospital)
        cat_scores[cat] = score
        summary_rows.append((label, score, n_total, d_min, has_hospital))

        # harita için
        df["cat"]=cat
        all_rows.append(df)

        # özet satırı
        dmin_txt = fmt_meters(d_min) if d_min is not None else "-"
        if cat == "health":
            hosp_txt = " (hastane: var)" if has_hospital else " (hastane: yok)"
        else:
            hosp_txt = ""
        print(f"   ⇒ Puan: {score:.1f}/10  | n={n_total}  | en yakın={dmin_txt}{hosp_txt}")

    # Genel puan (ağırlıklı)
    total_weight = sum(OVERALL_WEIGHTS.get(cat, 0.0) for cat in CATS.keys())
    overall = 0.0
    for cat in CATS.keys():
        w = OVERALL_WEIGHTS.get(cat, 0.0)
        overall += w * cat_scores.get(cat, 0.0)
    # Toplam ağırlık 1.0 varsayımıyla; yine de emniyet:
    if total_weight > 0:
        overall = overall / 1.0  # zaten ağırlıklar 1.0 topluyor
    print("\n=== Kategori Puanları ===")
    for label, score, n_total, d_min, has_hospital in summary_rows:
        print(f"{label:<8}: {score:>4.1f}/10")
    print(f"\n*** GENEL PUAN: {overall:.1f}/10 ***")

    # Harita
    if not all_rows:
        return
    big = pd.concat(all_rows, ignore_index=True)

    m = folium.Map(location=[lat, lon], zoom_start=15, control_scale=True)
    folium.Marker([lat, lon], popup=f"Adres: {disp}", tooltip="Adres",
                  icon=folium.Icon(color="black", icon="home")).add_to(m)
    for _, r in big.iterrows():
        label = CATS[r["cat"]]["label"]
        popup = (f"{label}: {r['name']}<br>"
                 f"Yürüme: {fmt_meters(r['walk_m'])}, {fmt_seconds(r['walk_s'])}<br>"
                 f"Araba: {fmt_meters(r['drive_m'])}, {fmt_seconds(r['drive_s'])}")
        folium.Marker([float(r["lat"]), float(r["lon"])],
                      popup=popup, tooltip=f"{label}: {r['name']}",
                      icon=folium.Icon(color=CATS[r["cat"]]["color"])).add_to(m)

    # Legend
    entries = "".join(
        f'<div style="display:flex;align-items:center;margin:2px 0;">'
        f'<span style="display:inline-block;width:12px;height:12px;background:{meta["color"]};margin-right:6px;border:1px solid #333;"></span>'
        f'{meta["label"]}</div>' for meta in CATS.values()
    )
    legend_html = (
        f'<div style="position:fixed;bottom:10px;left:10px;z-index:9999;background:#fff;padding:8px 10px;'
        f'border:1px solid #999;border-radius:6px;font-size:13px;">'
        f'<div style="font-weight:600;margin-bottom:4px;">Legenda</div>{entries}</div>'
    )
    m.get_root().html.add_child(folium.Element(legend_html))

    # Scorecard overlay (kategori puanları + genel)
    score_items = "".join(
        f'<div style="display:flex;justify-content:space-between;"><span>{label}</span>'
        f'<span>{score:0.1f}/10</span></div>'
        for (label, score, *_rest) in summary_rows
    )
    score_html = (
        f'<div style="position:fixed;top:10px;right:10px;z-index:9999;background:#fff;padding:10px 12px;'
        f'border:1px solid #999;border-radius:6px;font-size:13px;min-width:200px;">'
        f'<div style="font-weight:700;margin-bottom:6px;">Puanlama</div>'
        f'{score_items}'
        f'<hr style="margin:6px 0;border:none;border-top:1px solid #ddd;" />'
        f'<div style="display:flex;justify-content:space-between;font-weight:700;">'
        f'<span>Genel</span><span>{overall:0.1f}/10</span>'
        f'</div>'
        f'</div>'
    )
    m.get_root().html.add_child(folium.Element(score_html))

    out = os.path.abspath("map.html")
    m.save(out)
    print(f"\nHarita kaydedildi: {out}")

def analyze(address=None, lat=None, lon=None, radius=DEFAULT_RADIUS_M, topn=TOP_N,
            nodes_path="./cache/be_poi.parquet", polys_path="./cache/be_poi_poly.parquet"):
    # konum
    if address:
        lat, lon, disp = geocode(address)
    elif lat is not None and lon is not None:
        disp = f"({lat:.6f}, {lon:.6f})"
    else:
        raise ValueError("address veya (lat,lon) verin.")

    nodes_ok = nodes_path and os.path.exists(nodes_path)
    polys_ok = polys_path and os.path.exists(polys_path)
    if not nodes_ok and not polys_ok:
        raise FileNotFoundError("Ne node ne polygon cache bulundu.")

    con = duckdb.connect()
    all_rows = []
    cat_scores = {}
    summary_rows = []

    for cat in CATS.keys():
        df = query_category(con, nodes_path if nodes_ok else None,
                            polys_path if polys_ok else None,
                            cat, lat, lon, radius, topn)
        label = CATS[cat]["label"]
        if df.empty:
            cat_scores[cat] = 0.0
            summary_rows.append((label, 0.0, 0, None, False))
            continue

        n_total = int(df.iloc[0]["n_total"])
        d_min   = float(df.iloc[0]["d_min"]) if pd.notnull(df.iloc[0]["d_min"]) else None
        has_hospital = bool(df.iloc[0]["has_hospital_any"]) if "has_hospital_any" in df.columns else False
        score = calc_category_score(cat, n_total, d_min, has_hospital)
        cat_scores[cat] = score
        summary_rows.append((label, score, n_total, d_min, has_hospital))

        df["cat"] = cat
        all_rows.append(df)

    # genel puan (ağırlıklı)
    overall = 0.0
    for cat in CATS.keys():
        overall += OVERALL_WEIGHTS.get(cat, 0.0) * cat_scores.get(cat, 0.0)

    # harita
    m = folium.Map(location=[lat, lon], zoom_start=15, control_scale=True)
    folium.Marker([lat, lon], popup=f"Adres: {disp}", tooltip="Adres",
                  icon=folium.Icon(color="black", icon="home")).add_to(m)

    big = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    if not big.empty:
        for _, r in big.iterrows():
            label = CATS[r["cat"]]["label"]
            popup = (f"{label}: {r['name']}<br>"
                     f"Yürüme: {fmt_meters(r['walk_m'])}, {fmt_seconds(r['walk_s'])}<br>"
                     f"Araba: {fmt_meters(r['drive_m'])}, {fmt_seconds(r['drive_s'])}")
            folium.Marker([float(r["lat"]), float(r["lon"])],
                          popup=popup, tooltip=f"{label}: {r['name']}",
                          icon=folium.Icon(color=CATS[r["cat"]]["color"])).add_to(m)

    # Legend + Scorecard (harita üstü overlay)
    entries = "".join(
        f'<div style="display:flex;align-items:center;margin:2px 0;">'
        f'<span style="display:inline-block;width:12px;height:12px;background:{meta["color"]};margin-right:6px;border:1px solid #333;"></span>'
        f'{meta["label"]}</div>' for meta in CATS.values()
    )
    m.get_root().html.add_child(folium.Element(
        f'<div style="position:fixed;bottom:10px;left:10px;z-index:9999;background:#fff;padding:8px 10px;border:1px solid #999;border-radius:6px;font-size:13px;">'
        f'<div style="font-weight:600;margin-bottom:4px;">Legenda</div>{entries}</div>'
    ))
    score_items = "".join(
        f'<div style="display:flex;justify-content:space-between;"><span>{label}</span>'
        f'<span>{cat_scores.get(slug,0.0):0.1f}/10</span></div>'
        for slug,label in [(k, CATS[k]["label"]) for k in CATS.keys()]
    )
    m.get_root().html.add_child(folium.Element(
        f'<div style="position:fixed;top:10px;right:10px;z-index:9999;background:#fff;padding:10px 12px;'
        f'border:1px solid #999;border-radius:6px;font-size:13px;min-width:200px;">'
        f'<div style="font-weight:700;margin-bottom:6px;">Puanlama</div>'
        f'{score_items}'
        f'</div>'
    ))
    map_html = m.get_root().render()

    # tablo verileri
    results_by_cat = {}
    if not big.empty:
        for cat in CATS.keys():
            sub = big[big["cat"] == cat].copy()
            if sub.empty:
                results_by_cat[cat] = []
                continue
            rows = []
            for _, r in sub.iterrows():
                rows.append({
                    "name": r["name"],
                    "walk_m": fmt_meters(r["walk_m"]),
                    "walk_s": fmt_seconds(r["walk_s"]),
                    "drive_m": fmt_meters(r["drive_m"]),
                    "drive_s": fmt_seconds(r["drive_s"]),
                })
            results_by_cat[cat] = rows
    else:
        for cat in CATS.keys():
            results_by_cat[cat] = []

    cat_scores_pretty = {CATS[c]["label"]: float(f"{cat_scores.get(c,0.0):.1f}") for c in CATS.keys()}
    return {
        "display_address": disp,
        "lat": lat, "lon": lon,
        "radius": radius,
        "map_html": map_html,
        "scores": cat_scores_pretty,
        "overall": float(f"{(sum(OVERALL_WEIGHTS.get(c,0.0)*cat_scores.get(c,0.0) for c in CATS.keys())):.1f}"),
        "results": results_by_cat,
    }


if __name__ == "__main__":
    main()
