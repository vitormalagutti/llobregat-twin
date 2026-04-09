"""
Pure-Python shapefile → GeoJSON converter.
Handles: Polygon, MultiPolygon, LineString, MultiLineString, Point
Reprojects Drenaje from ETRS89/UTM-31N → WGS84 using ellipsoid math.
"""
import struct, json, math
from pathlib import Path

# ── UTM Zone 31N → WGS84 ──────────────────────────────────────────────────────
def utm31n_to_wgs84(E, N):
    """ETRS89 UTM Zone 31N (easting, northing) → (lon, lat) in WGS84 degrees."""
    k0   = 0.9996
    a    = 6378137.0
    f    = 1.0 / 298.257222101   # GRS80
    e2   = 2*f - f**2
    e    = math.sqrt(e2)
    ep2  = e2 / (1 - e2)
    FE   = 500000.0
    FN   = 0.0
    lon0 = math.radians(3.0)     # central meridian zone 31

    x = E - FE
    y = N - FN
    M = y / k0
    mu = M / (a * (1 - e2/4 - 3*e2**2/64 - 5*e2**3/256))
    e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))
    phi1 = (mu
            + (3*e1/2 - 27*e1**3/32) * math.sin(2*mu)
            + (21*e1**2/16 - 55*e1**4/32) * math.sin(4*mu)
            + (151*e1**3/96) * math.sin(6*mu)
            + (1097*e1**4/512) * math.sin(8*mu))
    N1    = a / math.sqrt(1 - e2*math.sin(phi1)**2)
    T1    = math.tan(phi1)**2
    C1    = ep2 * math.cos(phi1)**2
    R1    = a*(1-e2) / (1 - e2*math.sin(phi1)**2)**1.5
    D     = x / (N1 * k0)
    lat = phi1 - (N1*math.tan(phi1)/R1) * (
          D**2/2
          - (5 + 3*T1 + 10*C1 - 4*C1**2 - 9*ep2) * D**4/24
          + (61 + 90*T1 + 298*C1 + 45*T1**2 - 252*ep2 - 3*C1**2) * D**6/720)
    lon = lon0 + (
          D
          - (1 + 2*T1 + C1) * D**3/6
          + (5 - 2*C1 + 28*T1 - 3*C1**2 + 8*ep2 + 24*T1**2) * D**5/120
          ) / math.cos(phi1)
    return math.degrees(lon), math.degrees(lat)

# ── DBF reader ────────────────────────────────────────────────────────────────
def read_dbf(path):
    """Read a .dbf file and return a list of dicts (one per record)."""
    with open(str(path), 'rb') as f:
        data = f.read()
    n_records   = struct.unpack_from('<I', data, 4)[0]
    header_size = struct.unpack_from('<H', data, 8)[0]
    record_size = struct.unpack_from('<H', data, 10)[0]

    # Parse field descriptors (start at byte 32, each 32 bytes, terminated by 0x0D)
    fields = []
    off = 32
    while off < header_size - 1 and data[off] != 0x0D:
        name = data[off:off+11].split(b'\x00')[0].decode('latin-1').strip()
        ftype = chr(data[off+11])
        flen  = data[off+16]
        fields.append((name, ftype, flen))
        off += 32

    # Parse records
    records = []
    rec_off = header_size
    for _ in range(n_records):
        if rec_off + record_size > len(data):
            break
        deleted = data[rec_off] == 0x2A   # '*' marks deleted
        rec_off += 1
        row = {}
        for fname, ftype, flen in fields:
            raw = data[rec_off:rec_off+flen].decode('latin-1').strip()
            if not deleted:
                if ftype == 'N':
                    try:
                        row[fname] = float(raw) if '.' in raw else int(raw)
                    except ValueError:
                        row[fname] = None
                else:
                    row[fname] = raw
            rec_off += flen
        if not deleted:
            records.append(row)
        else:
            rec_off += sum(f[2] for f in fields) - sum(f[2] for f in fields)  # skip already advanced
    return records


# ── Shapefile reader ──────────────────────────────────────────────────────────
def read_shp(path, reproject=None):
    with open(str(path), 'rb') as f:
        data = f.read()
    shape_type = struct.unpack_from('<i', data, 32)[0]
    xmin, ymin, xmax, ymax = struct.unpack_from('<4d', data, 36)
    features = []
    offset = 100
    while offset + 8 <= len(data):
        rec_num      = struct.unpack_from('>i', data, offset)[0]
        content_len  = struct.unpack_from('>i', data, offset+4)[0] * 2
        offset += 8
        if offset + content_len > len(data):
            break
        st = struct.unpack_from('<i', data, offset)[0]
        if st == 0:
            offset += content_len; continue

        def pts(base, n):
            raw = [(struct.unpack_from('<2d', data, base + i*16))
                   for i in range(n)]
            if reproject:
                return [list(reproject(p[0], p[1])) for p in raw]
            return [[p[0], p[1]] for p in raw]

        if st == 1:                         # Point
            x, y = struct.unpack_from('<2d', data, offset+4)
            coord = list(reproject(x, y)) if reproject else [x, y]
            geom = {'type':'Point','coordinates':coord}

        elif st in (3, 23):                 # Polyline / PolylineM
            np_ = struct.unpack_from('<i', data, offset+36)[0]
            npts = struct.unpack_from('<i', data, offset+40)[0]
            parts = list(struct.unpack_from(f'<{np_}i', data, offset+44))
            pbase = offset + 44 + np_*4
            all_pts = pts(pbase, npts)
            rings = [all_pts[parts[i]:(parts[i+1] if i+1<np_ else npts)]
                     for i in range(np_)]
            if np_ == 1:
                geom = {'type':'LineString','coordinates':rings[0]}
            else:
                geom = {'type':'MultiLineString','coordinates':rings}

        elif st in (5, 15, 25):            # Polygon / PolygonZ / PolygonM
            np_ = struct.unpack_from('<i', data, offset+36)[0]
            npts = struct.unpack_from('<i', data, offset+40)[0]
            parts = list(struct.unpack_from(f'<{np_}i', data, offset+44))
            pbase = offset + 44 + np_*4
            all_pts = pts(pbase, npts)
            rings = [all_pts[parts[i]:(parts[i+1] if i+1<np_ else npts)]
                     for i in range(np_)]
            geom = {'type':'Polygon','coordinates':rings}
        else:
            offset += content_len; continue

        features.append({'type':'Feature','geometry':geom,'properties':None})
        offset += content_len

    # Load matching DBF for attributes
    dbf_path = Path(str(path).replace('.shp', '.dbf'))
    if dbf_path.exists():
        records = read_dbf(dbf_path)
        for i, feat in enumerate(features):
            feat['properties'] = records[i] if i < len(records) else {}
    else:
        for feat in features:
            feat['properties'] = {}

    return {'type':'FeatureCollection','features':features,
            'metadata':{'bounds':[xmin,ymin,xmax,ymax],'shape_type':shape_type}}

# ── Simplify Drenaje (keep every Nth point for smaller GeoJSON) ──────────────
def simplify_lines(fc, step=2):
    """Keep every `step`th coordinate to reduce file size."""
    for feat in fc['features']:
        g = feat['geometry']
        if g['type'] == 'LineString':
            c = g['coordinates']
            g['coordinates'] = c[::step] + ([c[-1]] if c[-1] != c[::step][-1] else [])
        elif g['type'] == 'MultiLineString':
            g['coordinates'] = [
                (c[::step] + ([c[-1]] if c and c[-1] != c[::step][-1] else []))
                for c in g['coordinates']
            ]
    return fc

# ── Run conversions ───────────────────────────────────────────────────────────
shps_dir = Path(__file__).parent

print("Reading Cuenca_Llobregat (WGS84)…")
cuenca = read_shp(shps_dir / 'Cuenca_Llobregat.shp')
print(f"  {len(cuenca['features'])} features, bounds: {cuenca['metadata']['bounds']}")

print("Reading Drenaje (UTM 31N → WGS84)…")
drenaje = read_shp(shps_dir / 'Drenaje.shp', reproject=utm31n_to_wgs84)
print(f"  {len(drenaje['features'])} features (before simplify)")
drenaje = simplify_lines(drenaje, step=3)
print(f"  Simplified to step=3 for smaller file size")

print("Reading modelo_flujo (WGS84)…")
modelo  = read_shp(shps_dir / 'modelo_flujo.shp')
print(f"  {len(modelo['features'])} features, bounds: {modelo['metadata']['bounds']}")

# Write GeoJSON files
for name, fc in [('Cuenca_Llobregat', cuenca),
                 ('Drenaje',           drenaje),
                 ('modelo_flujo',      modelo)]:
    out = shps_dir / f'{name}.geojson'
    with open(out, 'w') as f:
        json.dump(fc, f, separators=(',', ':'))
    size_kb = out.stat().st_size / 1024
    print(f"Wrote {out.name}  ({size_kb:.1f} KB)")

print("\nDone.")
