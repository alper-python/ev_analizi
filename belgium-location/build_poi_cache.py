# build_poi_cache.py — ÜLKE GENELİ ÖN-İŞLEME (osmium + sık flush + progress)
import os, argparse, time
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import osmium as osm  # Python Osmium

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

def categorize(tags):
    out=[]
    a=tags.get("amenity"); s=tags.get("shop"); h=tags.get("healthcare")
    r=tags.get("railway"); hw=tags.get("highway"); pt=tags.get("public_transport")
    l=tags.get("leisure"); b=tags.get("boundary"); lu=tags.get("landuse"); sp=tags.get("sport")
    if (a in {"school","college","kindergarten"}) or ("school:level" in tags) or ("isced:level" in tags):
        out.append("school")
    if (s in {"supermarket","convenience"}) or (a == "marketplace"):
        out.append("market")
    if (a in {"hospital","clinic","doctors","pharmacy","dentist"}) or (h in HEALTHCARE_OK) or ("healthcare" in tags):
        out.append("health")
    if (r in RAILWAY_OK) or (a == "bus_station") or (hw == "bus_stop") or (pt in PT_OK):
        out.append("transit")
    if (l in {"park","garden","recreation_ground","playground","nature_reserve"}) or (b == "national_park") or (lu == "grass") or ("protect_class" in tags):
        out.append("park")
    if (l in {"fitness_centre","sports_centre"}) or (a == "gym") or (sp in {"fitness","gym"}) or ("sport" in tags):
        out.append("sport")
    return out

SCHEMA = pa.schema([
    ("id", pa.int64()), ("cat", pa.string()), ("name", pa.string()),
    ("lat", pa.float64()), ("lon", pa.float64()),
    ("amenity", pa.string()), ("shop", pa.string()), ("healthcare", pa.string()),
    ("railway", pa.string()), ("highway", pa.string()), ("public_transport", pa.string()),
    ("leisure", pa.string()), ("boundary", pa.string()), ("landuse", pa.string()),
    ("sport", pa.string()), ("school_level", pa.string()), ("isced_level", pa.string()),
])

class POIHandler(osm.SimpleHandler):
    def __init__(self, writer, out_path, batch_size=50_000, progress_every=250_000):
        super().__init__()
        self.writer = writer
        self.out_path = out_path
        self.batch = []
        self.batch_size = batch_size
        self.progress_every = progress_every
        self.count_in = 0
        self.count_out = 0
        self.t0 = time.time()

    def node(self, n):
        self.count_in += 1
        if (self.count_in % self.progress_every) == 0:
            size = os.path.getsize(self.out_path) if os.path.exists(self.out_path) else 0
            print(f"[PROGRESS] read={self.count_in:,}  matched={self.count_out:,}  file={size/1e6:.1f} MB  elapsed={time.time()-self.t0:.1f}s")

        if not n.location.valid(): 
            return

        t = {k:v for k,v in n.tags}
        if not (("amenity" in t) or ("shop" in t) or ("healthcare" in t) or
                ("railway" in t) or ("highway" in t) or ("public_transport" in t) or
                ("leisure" in t) or ("boundary" in t) or ("landuse" in t) or ("sport" in t) or
                ("school:level" in t) or ("isced:level" in t)):
            return

        cats = categorize(t)
        if not cats:
            return

        name = t.get("name") or t.get("ref") or None
        base = {
            "id": n.id, "name": name,
            "lat": float(n.location.lat), "lon": float(n.location.lon),
            "amenity": t.get("amenity"), "shop": t.get("shop"), "healthcare": t.get("healthcare"),
            "railway": t.get("railway"), "highway": t.get("highway"), "public_transport": t.get("public_transport"),
            "leisure": t.get("leisure"), "boundary": t.get("boundary"), "landuse": t.get("landuse"),
            "sport": t.get("sport"), "school_level": t.get("school:level"), "isced_level": t.get("isced:level"),
        }
        for c in cats:
            row = dict(base); row["cat"] = c
            self.batch.append(row); self.count_out += 1

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
    ap = argparse.ArgumentParser(description="Belgium PBF -> POI Parquet cache (ülke geneli).")
    ap.add_argument("--pbf", required=True, help="belgium-latest.osm.pbf yolu")
    ap.add_argument("--out", default="cache/be_poi.parquet", help="Parquet çıktı")
    ap.add_argument("--batch", type=int, default=50_000, help="Flush batch boyutu")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    print("[INFO] PBF okunuyor, bu işlem tek seferlik…")
    t0 = time.time()
    writer = pq.ParquetWriter(args.out, SCHEMA, compression="zstd")

    try:
        h = POIHandler(writer, out_path=args.out, batch_size=args.batch)
        h.apply_file(args.pbf, locations=True)
        h.flush()
    finally:
        writer.close()

    dt = time.time()-t0
    size_mb = os.path.getsize(args.out)/1e6 if os.path.exists(args.out) else 0
    print(f"[DONE] input_nodes~{h.count_in:,}  matched_rows={h.count_out:,}  file={size_mb:.1f} MB  time={dt/60:.1f} dk")

if __name__ == "__main__":
    main()
