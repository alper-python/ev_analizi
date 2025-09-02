# build_poi_poly_cache_osmium.py — ülke geneli polygon/multipolygon POI centroid cache (osmium + shapely)
# Gereken: pip install osmium shapely pyarrow pandas

import os, time, argparse, hashlib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import osmium as osm
from shapely import wkb

# İlgili etiket kümeleri
AMENITY_OK = {"school","college","kindergarten","marketplace","hospital","clinic","doctors","pharmacy","dentist","bus_station","gym"}
SHOP_OK = {"supermarket","convenience"}
HEALTHCARE_OK = {"hospital","clinic","doctor","dentist","physiotherapist","laboratory","blood_donation"}
RAILWAY_OK = {"station","halt","tram_stop","subway_entrance"}
HIGHWAY_OK = {"bus_stop"}
PT_OK = {"platform","stop_position"}
LEISURE_OK = {"park","garden","recreation_ground","playground","nature_reserve","fitness_centre","sports_centre"}
BOUNDARY_OK = {"national_park"}
LANDUSE_OK = {"grass"}
SPORT_OK = {"fitness","gym"}

SCHEMA = pa.schema([
    ("uid", pa.string()), ("cat", pa.string()), ("name", pa.string()), ("brand", pa.string()),
    ("lat", pa.float64()), ("lon", pa.float64()),
    ("amenity", pa.string()), ("shop", pa.string()), ("healthcare", pa.string()),
    ("railway", pa.string()), ("highway", pa.string()), ("public_transport", pa.string()),
    ("leisure", pa.string()), ("boundary", pa.string()), ("landuse", pa.string()),
    ("sport", pa.string()), ("school_level", pa.string()), ("isced_level", pa.string()),
])

def categorize(t: dict):
    out=[]
    a=t.get("amenity"); s=t.get("shop"); h=t.get("healthcare")
    r=t.get("railway"); hw=t.get("highway"); pt=t.get("public_transport")
    l=t.get("leisure"); b=t.get("boundary"); lu=t.get("landuse"); sp=t.get("sport")
    if (a in {"school","college","kindergarten"}) or ("school:level" in t) or ("isced:level" in t): out.append("school")
    if (s in {"supermarket","convenience"}) or (a=="marketplace"): out.append("market")
    if (a in {"hospital","clinic","doctors","pharmacy","dentist"}) or (h in HEALTHCARE_OK) or ("healthcare" in t): out.append("health")
    if (r in RAILWAY_OK) or (a=="bus_station") or (hw=="bus_stop") or (pt in PT_OK): out.append("transit")
    if (l in {"park","garden","recreation_ground","playground","nature_reserve"}) or (b=="national_park") or (lu=="grass") or ("protect_class" in t): out.append("park")
    if (l in {"fitness_centre","sports_centre"}) or (a=="gym") or (sp in {"fitness","gym"}) or ("sport" in t): out.append("sport")
    return out

def stable_uid(name, lat, lon):
    s = f"{(name or '').strip().lower()}|{round(float(lat),6)}|{round(float(lon),6)}"
    return hashlib.md5(s.encode("utf-8")).hexdigest()

class PolyHandler(osm.SimpleHandler):
    """
    Multipolygon relation + kapalı way'lerden oluşan 'area' nesnelerini yakalar,
    centroid üretir ve ilgilendiğimiz kategorilere göre satırlaştırır.
    """
    def __init__(self, writer, out_path, batch_size=50_000, progress_every=100_000):
        super().__init__()
        self.writer = writer
        self.out_path = out_path
        self.batch = []
        self.batch_size = batch_size
        self.progress_every = progress_every
        self.count_in = 0
        self.count_out = 0
        self.seen = set()  # (name_lower, round(lat,4), round(lon,4)) ile dupe kırp
        self.wkbf = osm.geom.WKBFactory()
        self.t0 = time.time()

    def area(self, a):
        self.count_in += 1
        if (self.count_in % self.progress_every) == 0:
            size = os.path.getsize(self.out_path) if os.path.exists(self.out_path) else 0
            print(f"[PROGRESS] read_areas={self.count_in:,}  matched={self.count_out:,}  file={size/1e6:.1f} MB  elapsed={time.time()-self.t0:.1f}s")

        tags = {k:v for k,v in a.tags}
        # hızlı ön-eleme: ilgili anahtarlardan hiçbiri yoksa çık
        if not (("amenity" in tags) or ("shop" in tags) or ("healthcare" in tags) or
                ("railway" in tags) or ("highway" in tags) or ("public_transport" in tags) or
                ("leisure" in tags) or ("boundary" in tags) or ("landuse" in tags) or ("sport" in tags) or
                ("school:level" in tags) or ("isced:level" in tags)):
            return

        cats = categorize(tags)
        if not cats:
            return

        # geometri -> centroid
        try:
            # multipolygon tercih; olmazsa polygon dene
            try:
                g_wkb = self.wkbf.create_multipolygon(a)
            except Exception:
                g_wkb = self.wkbf.create_polygon(a)
            geom = wkb.loads(g_wkb)  # shapely geometry
            c = geom.centroid
            lat, lon = float(c.y), float(c.x)
        except Exception:
            return  # bozuk geometri vb.

        name = tags.get("name") or tags.get("brand") or tags.get("ref") or None
        nm_key = ((name or "").strip().lower(), round(lat,4), round(lon,4))
        if nm_key in self.seen:
            return
        self.seen.add(nm_key)

        base = {
            "name": name,
            "brand": tags.get("brand"),
            "lat": lat, "lon": lon,
            "amenity": tags.get("amenity"),
            "shop": tags.get("shop"),
            "healthcare": tags.get("healthcare"),
            "railway": tags.get("railway"),
            "highway": tags.get("highway"),
            "public_transport": tags.get("public_transport"),
            "leisure": tags.get("leisure"),
            "boundary": tags.get("boundary"),
            "landuse": tags.get("landuse"),
            "sport": tags.get("sport"),
            "school_level": tags.get("school:level"),
            "isced_level": tags.get("isced:level"),
        }
        uid = stable_uid(name, lat, lon)
        for c in cats:
            row = {"uid": uid, "cat": c, **base}
            self.batch.append(row)
            self.count_out += 1

        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.batch:
            return
        df = pd.DataFrame(self.batch)
        table = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)
        self.writer.write_table(table)
        size = os.path.getsize(self.out_path) if os.path.exists(self.out_path) else 0
        print(f"[FLUSH] wrote_rows={len(df):,}  total_matched={self.count_out:,}  file_now={size/1e6:.1f} MB")
        self.batch.clear()

def main():
    ap = argparse.ArgumentParser(description="Belgium PBF -> polygon/multipolygon centroid cache (osmium)")
    ap.add_argument("--pbf", required=True, help="belgium-latest.osm.pbf yolu")
    ap.add_argument("--out", default="cache/be_poi_poly.parquet", help="Parquet çıktı yolu")
    ap.add_argument("--batch", type=int, default=50_000, help="Flush batch boyutu")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    print("[INFO] PBF okunuyor (areas), bu işlem tek seferlik…")
    t0 = time.time()
    writer = pq.ParquetWriter(args.out, SCHEMA, compression="zstd")

    try:
        h = PolyHandler(writer, out_path=args.out, batch_size=args.batch)
        # areas oluşturmak için locations=True gerekli
        h.apply_file(args.pbf, locations=True)
        h.flush()
    finally:
        writer.close()

    dt = time.time()-t0
    size_mb = os.path.getsize(args.out)/1e6 if os.path.exists(args.out) else 0
    print(f"[DONE] areas_read~{h.count_in:,}  rows={h.count_out:,}  file={size_mb:.1f} MB  time={dt/60:.1f} dk")

if __name__ == "__main__":
    main()
