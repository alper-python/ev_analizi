# server.py — Ev Çevresi Analizi web API'si
# Çalıştırma: python server.py  (http://127.0.0.1:5000)
import os
import time
import threading

from flask import Flask, jsonify, request, send_from_directory

from app_duckdb import (DEFAULT_LANG, DEFAULT_NODES, DEFAULT_POLYS,
                        DEFAULT_RADIUS_M, SUPPORTED_LANGS, TOP_N, cat_label,
                        geocode, run_analysis)

NODES_PATH = os.environ.get("POI_NODES", DEFAULT_NODES)
POLYS_PATH = os.environ.get("POI_POLYS", DEFAULT_POLYS)

MIN_RADIUS_M, MAX_RADIUS_M = 200, 10_000
MAX_TOPN = 20

# Kullanıcıya dönen mesajlar (tr/en/nl). Analiz sonucu dilden bağımsız cache'lenir;
# etiket ve mesajlar yanıt üretilirken lang'e göre seçilir.
MESSAGES = {
    "bad_number": {
        "tr": "radius ve topn sayı olmalı.",
        "en": "radius and topn must be numbers.",
        "nl": "radius en topn moeten getallen zijn.",
    },
    "bad_radius": {
        "tr": "radius {lo}-{hi} m aralığında olmalı.",
        "en": "radius must be between {lo} and {hi} m.",
        "nl": "radius moet tussen {lo} en {hi} m liggen.",
    },
    "bad_topn": {
        "tr": "topn 1-{hi} aralığında olmalı.",
        "en": "topn must be between 1 and {hi}.",
        "nl": "topn moet tussen 1 en {hi} liggen.",
    },
    "address_not_found": {
        "tr": "Adres bulunamadı. Adresi posta koduyla birlikte yazmayı deneyin.",
        "en": "Address not found. Try including the postal code.",
        "nl": "Adres niet gevonden. Probeer het adres met postcode.",
    },
    "geocode_unreachable": {
        "tr": "Geocode servisine ulaşılamadı, lütfen tekrar deneyin.",
        "en": "Could not reach the geocoding service, please try again.",
        "nl": "Geocodeerservice is niet bereikbaar, probeer het opnieuw.",
    },
    "bad_coords": {
        "tr": "Geçersiz koordinat.",
        "en": "Invalid coordinates.",
        "nl": "Ongeldige coördinaten.",
    },
    "need_input": {
        "tr": "address veya lat+lon verin.",
        "en": "Provide address or lat+lon.",
        "nl": "Geef een adres of lat+lon op.",
    },
    "cache_missing": {
        "tr": "POI cache dosyaları bulunamadı; sunucu henüz hazır değil.",
        "en": "POI cache files not found; the server is not ready yet.",
        "nl": "POI-cachebestanden niet gevonden; de server is nog niet gereed.",
    },
    "no_results": {
        "tr": "Bu yarıçapta sonuç bulunamadı; yarıçapı büyütmeyi deneyin.",
        "en": "No results within this radius; try increasing the radius.",
        "nl": "Geen resultaten binnen deze straal; probeer een grotere straal.",
    },
}

def msg(key, lang, **kw):
    texts = MESSAGES[key]
    return texts.get(lang, texts[DEFAULT_LANG]).format(**kw)

# Aynı konum+yarıçap için sorguyu tekrarlamamak üzere basit in-memory cache
CACHE_TTL_S = 3600
_result_cache = {}
_cache_lock = threading.Lock()

app = Flask(__name__, static_folder="static", static_url_path="")


def _bad_request(msg):
    return jsonify({"error": msg}), 400


def _to_minutes(seconds):
    return int(round(seconds / 60))


def _public_payload(result, display_address, lang):
    """run_analysis çıktısını API veri sözleşmesine çevirir (bkz. docs/FRONTEND_DESIGN_BRIEF.md).
    Etiketler burada lang'e göre seçilir; cache'teki sonuç dilden bağımsızdır."""
    categories = []
    for c in result["categories"]:
        categories.append({
            "key": c["key"],
            "label": cat_label(c["key"], lang),
            "score": c["score"],
            "count": c["count"],
            "nearest_m": c["nearest_m"],
            "has_hospital": c["has_hospital"],
            "items": [{
                "name": it["name"],
                "brand": it["brand"],
                "lat": it["lat"], "lon": it["lon"],
                "walk_m": int(round(it["walk_m"])),
                "walk_min": _to_minutes(it["walk_s"]),
                "drive_m": int(round(it["drive_m"])),
                "drive_min": _to_minutes(it["drive_s"]),
            } for it in c["items"]],
        })
    return {
        "display_address": display_address,
        "lat": result["lat"], "lon": result["lon"],
        "radius": result["radius"],
        "lang": lang,
        "overall": result["overall"],
        "categories": categories,
    }


def _cached_analysis(lat, lon, radius, topn):
    key = (round(lat, 5), round(lon, 5), radius, topn)
    now = time.time()
    with _cache_lock:
        hit = _result_cache.get(key)
        if hit and now - hit[0] < CACHE_TTL_S:
            return hit[1]
    result = run_analysis(lat, lon, radius=radius, topn=topn,
                          nodes_path=NODES_PATH, polys_path=POLYS_PATH)
    with _cache_lock:
        _result_cache[key] = (now, result)
    return result


@app.get("/api/health")
def health():
    nodes_ok = os.path.exists(NODES_PATH)
    polys_ok = os.path.exists(POLYS_PATH)
    return jsonify({
        "status": "ok" if (nodes_ok or polys_ok) else "no_cache",
        "cache": {"nodes": nodes_ok, "polys": polys_ok},
    })


@app.post("/api/analyze")
def analyze():
    body = request.get_json(silent=True) or {}

    lang = str(body.get("lang", DEFAULT_LANG)).lower()
    if lang not in SUPPORTED_LANGS:
        return _bad_request(f"Unsupported lang: {lang}. Supported: {', '.join(SUPPORTED_LANGS)}")

    try:
        radius = int(body.get("radius", DEFAULT_RADIUS_M))
        topn = int(body.get("topn", TOP_N))
    except (TypeError, ValueError):
        return _bad_request(msg("bad_number", lang))
    if not (MIN_RADIUS_M <= radius <= MAX_RADIUS_M):
        return _bad_request(msg("bad_radius", lang, lo=MIN_RADIUS_M, hi=MAX_RADIUS_M))
    if not (1 <= topn <= MAX_TOPN):
        return _bad_request(msg("bad_topn", lang, hi=MAX_TOPN))

    address = body.get("address")
    lat, lon = body.get("lat"), body.get("lon")

    if address:
        try:
            lat, lon, display_address = geocode(str(address))
        except RuntimeError:
            return _bad_request(msg("address_not_found", lang))
        except Exception:
            return jsonify({"error": msg("geocode_unreachable", lang)}), 502
    elif lat is not None and lon is not None:
        try:
            lat, lon = float(lat), float(lon)
        except (TypeError, ValueError):
            return _bad_request(msg("bad_coords", lang))
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return _bad_request(msg("bad_coords", lang))
        display_address = f"({lat:.6f}, {lon:.6f})"
    else:
        return _bad_request(msg("need_input", lang))

    try:
        result = _cached_analysis(lat, lon, radius, topn)
    except FileNotFoundError:
        return jsonify({"error": msg("cache_missing", lang)}), 503

    payload = _public_payload(result, display_address, lang)
    if all(c["count"] == 0 for c in payload["categories"]):
        payload["note"] = msg("no_results", lang)
    return jsonify(payload)


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
