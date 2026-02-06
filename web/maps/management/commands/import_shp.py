import re
import shutil
import tempfile
import zipfile
import csv
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import quote_plus

import geopandas as gpd
import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from sqlalchemy import create_engine, text
from geoalchemy2 import Geometry


VECTOR_EXTS = {".shp", ".geojson", ".json", ".gpkg", ".kml", ".kmz", ".gml", ".zip"}

RESERVED_COLS = {"gid", "geom"}

def table_exists(engine, schema: str, table: str) -> bool:
    sql = """
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = :schema AND table_name = :table
    ) AS exists;
    """
    with engine.begin() as conn:
        return bool(conn.execute(text(sql), {"schema": schema, "table": table}).scalar())


def run_post_sql(engine, schema: str = "public", table: str):
    """
    Post-proceso para hex5km:
    - agrega geom_3857 si no existe
    - rellena geom_3857 con ST_Transform(geom, 3857)
    - crea índice GIST
    - ANALYZE
    """
    if not table_exists(engine, schema, table):
        return ("SKIP", f"No existe {schema}.{table}; no ejecuto post-SQL.")

    full_table = f"{quote_ident(schema)}.{quote_ident(table)}"

    statements = [
        f"""
        ALTER TABLE {full_table}
          ADD COLUMN IF NOT EXISTS geom_3857 geometry(MultiPolygon, 3857);
        """,
        f"""
        UPDATE {full_table}
        SET geom_3857 = ST_Transform(geom, 3857)
        WHERE geom_3857 IS NULL AND geom IS NOT NULL;
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {quote_ident(table + "_geom_3857_gix")}
        ON {full_table}
        USING GIST (geom_3857);
        """,
        f"ANALYZE {full_table};",
    ]

    with engine.begin() as conn:
        for st in statements:
            conn.execute(text(st))

    return ("OK", f"Post-SQL ejecutado para {schema}.{table} (geom_3857/idx/analyze).")


def slug_table_name(name: str) -> str:
    name = (name or "").lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "layer"
    return name[:63]  # límite postgres


def normalize_columns(cols) -> Dict[str, str]:
    mapping = {}
    used = set()
    for c in cols:
        if c == "geometry":
            mapping[c] = c
            continue
        new = slug_table_name(str(c))
        if new == "geometry":
            new = "attr_geometry"
        if new in {"gid", "geom"}:
            new = f"src_{new}"
        base = new
        i = 1
        while new in used:
            i += 1
            new = f"{base}_{i}"
        used.add(new)
        mapping[c] = new
    return mapping


def quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def pandas_dtype_to_pg(dtype) -> str:
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    return "TEXT"


def detect_geom_type_and_srid(
    gdf: gpd.GeoDataFrame,
    target_srid: Optional[int]
) -> Tuple[str, int, gpd.GeoDataFrame]:
    srid = 0
    if gdf.crs is not None:
        epsg = gdf.crs.to_epsg()
        if epsg is not None:
            srid = int(epsg)

    if target_srid is not None:
        if gdf.crs is None:
            srid = target_srid
        else:
            if srid != target_srid:
                gdf = gdf.to_crs(epsg=target_srid)
                srid = target_srid

    geom_types = set(t.upper() for t in gdf.geometry.geom_type.dropna().unique())
    if len(geom_types) == 1:
        geom_type = next(iter(geom_types)).replace(" ", "")
    else:
        geom_type = "GEOMETRY"

    return geom_type, srid, gdf


def read_vector(path: Path) -> gpd.GeoDataFrame:
    ext = path.suffix.lower()

    # KMZ -> extraer KML
    if ext == ".kmz":
        with zipfile.ZipFile(path, "r") as zf, tempfile.TemporaryDirectory() as td:
            zf.extractall(td)
            kmls = list(Path(td).rglob("*.kml"))
            if not kmls:
                raise RuntimeError(f"No encontré .kml dentro de {path.name}")
            return gpd.read_file(kmls[0])

    # ZIP (muy común para shapefile)
    if ext == ".zip":
        with zipfile.ZipFile(path, "r") as zf, tempfile.TemporaryDirectory() as td:
            zf.extractall(td)
            root = Path(td)

            shps = list(root.rglob("*.shp"))
            if shps:
                return gpd.read_file(shps[0])

            geojsons = list(root.rglob("*.geojson")) + list(root.rglob("*.json"))
            if geojsons:
                return gpd.read_file(geojsons[0])

            gpkgs = list(root.rglob("*.gpkg"))
            if gpkgs:
                return gpd.read_file(gpkgs[0])

            raise RuntimeError(f"ZIP {path.name} no contiene .shp/.geojson/.gpkg legibles")

    return gpd.read_file(path)


def build_create_table_sql(schema: str, table: str, gdf: gpd.GeoDataFrame, geom_type: str, srid: int) -> str:
    cols_sql = [f'{quote_ident("gid")} BIGSERIAL PRIMARY KEY']

    for col in gdf.columns:
        if col == "geometry":
            continue
        pg_type = pandas_dtype_to_pg(gdf[col].dtype)
        cols_sql.append(f"{quote_ident(col)} {pg_type}")

    cols_sql.append(f'{quote_ident("geom")} geometry({geom_type},{srid})')

    full_table = f"{quote_ident(schema)}.{quote_ident(table)}"
    sql = []
    sql.append(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)};")
    sql.append(f"CREATE TABLE IF NOT EXISTS {full_table} (\n  " + ",\n  ".join(cols_sql) + "\n);")
    sql.append(
        f"CREATE INDEX IF NOT EXISTS {quote_ident(table + '_geom_gix')} "
        f"ON {full_table} USING GIST ({quote_ident('geom')});"
    )
    return "\n".join(sql) + "\n"


def sqlalchemy_url_from_django(alias: str) -> str:
    db = settings.DATABASES.get(alias)
    if not db:
        raise CommandError(f"No existe DATABASES['{alias}'] en settings.")
    if db.get("ENGINE") not in (
        "django.contrib.gis.db.backends.postgis",
        "django.db.backends.postgresql",
        "django.db.backends.postgresql_psycopg2",
    ):
        raise CommandError(f"Este comando espera Postgres. ENGINE actual: {db.get('ENGINE')}")
    name = db.get("NAME") or ""
    user = db.get("USER") or ""
    password = db.get("PASSWORD") or ""
    host = db.get("HOST") or "localhost"
    port = db.get("PORT") or "5432"
    user_q = quote_plus(user)
    pass_q = quote_plus(password)
    return f"postgresql+psycopg2://{user_q}:{pass_q}@{host}:{port}/{name}"


def ensure_postgis(engine):
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))

def rename_reserved_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Evita colisiones con columnas que el script crea:
    - gid (PK)
    - geom (columna geometría PostGIS)
    Si existen en atributos del input, se renombran (src_gid, src_geom, etc.)
    """
    rename_map = {}
    for c in list(gdf.columns):
        if c == "geometry":
            continue
        c_norm = str(c).lower()
        if c_norm in RESERVED_COLS:
            rename_map[c] = f"src_{c_norm}"
    if rename_map:
        gdf = gdf.rename(columns=rename_map)
    return gdf

def repair_table_geometries(engine, schema: str, table: str, geom_col: str = "geom", snap: float = 1e-9):
    full_t = f'{quote_ident(schema)}.{quote_ident(table)}'
    g = quote_ident(geom_col)

    sql = f"""
    -- Reparar solo las inválidas
    UPDATE {full_t}
    SET {g} = ST_SnapToGrid(ST_MakeValid({g}), {snap})
    WHERE {g} IS NOT NULL AND NOT ST_IsValid({g});

    -- En caso de que MakeValid devuelva GEOMETRYCOLLECTION, intenta quedarte con lo “de tu tipo”:
    -- (esto no cambia nada si ya es un tipo simple)
    UPDATE {full_t}
    SET {g} = ST_CollectionExtract({g}, 
        CASE
          WHEN GeometryType({g}) IN ('POLYGON','MULTIPOLYGON') THEN 3
          WHEN GeometryType({g}) IN ('LINESTRING','MULTILINESTRING') THEN 2
          WHEN GeometryType({g}) IN ('POINT','MULTIPOINT') THEN 1
          ELSE 0
        END
    )
    WHERE {g} IS NOT NULL AND GeometryType({g}) = 'GEOMETRYCOLLECTION';
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

def geometry_validity_stats(engine, schema: str, table: str, geom_col: str = "geom"):
    full_t = f'{quote_ident(schema)}.{quote_ident(table)}'
    g = quote_ident(geom_col)
    sql = f"""
    SELECT
      COUNT(*) FILTER (WHERE {g} IS NULL) AS n_null,
      COUNT(*) FILTER (WHERE {g} IS NOT NULL AND ST_IsValid({g})) AS n_valid,
      COUNT(*) FILTER (WHERE {g} IS NOT NULL AND NOT ST_IsValid({g})) AS n_invalid
    FROM {full_t};
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql)).mappings().first()
    return row

def ingest_one(
    engine,
    path: Path,
    schema: str,
    table_prefix: str,
    if_exists: str,
    target_srid: Optional[int],
    out_sql_dir: Optional[Path],
    table_name_override: Optional[str] = None,
):
    gdf = read_vector(path)

    if gdf.empty:
        return ("SKIP", f"{path.name}: capa vacía")

    mapping = normalize_columns(gdf.columns)
    gdf = gdf.rename(columns=mapping)

    gdf = rename_reserved_columns(gdf)

    if "geometry" not in gdf:
        raise RuntimeError(f"{path.name}: no trae columna geometry")

    geom_type, srid, gdf = detect_geom_type_and_srid(gdf, target_srid)

    if table_name_override:
        table = slug_table_name(table_name_override)
    else:
        base = slug_table_name(path.stem)
        table = slug_table_name(f"{table_prefix}{base}")

    create_sql = build_create_table_sql(schema, table, gdf, geom_type, srid)

    if out_sql_dir is not None:
        out_sql_dir.mkdir(parents=True, exist_ok=True)
        (out_sql_dir / f"{schema}.{table}.sql").write_text(create_sql, encoding="utf-8")

    with engine.begin() as conn:
        if if_exists == "replace":
            conn.execute(text(f"DROP TABLE IF EXISTS {quote_ident(schema)}.{quote_ident(table)} CASCADE;"))
        conn.execute(text(create_sql))

    gdf = gdf.copy()

    # Asegura que la geometría activa sea la actual (por si no se llama "geometry")
    gdf = gdf.set_geometry(gdf.geometry.name)

    # Copia la geometría a una columna llamada "geom"
    gdf["geom"] = gdf.geometry

    # activa "geom" la geometría
    gdf = gdf.set_geometry("geom")

    old_geom = "geometry"
    if old_geom in gdf.columns and old_geom != "geom":
        gdf = gdf.drop(columns=[old_geom])

    dtype = {"geom": Geometry(geometry_type=geom_type, srid=srid)}
    gdf.to_postgis(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        dtype=dtype,
        chunksize=5000,
    )

    # Repara geometrías con problemas
    before = geometry_validity_stats(engine, schema, table, geom_col="geom")
    repair_table_geometries(engine, schema, table, geom_col="geom", snap=1e-9)
    #after = geometry_validity_stats(engine, schema, table, geom_col="geom")

    # Ejecuta post-SQL para hex5km (solo si existe)
    st2, msg2 = run_post_sql(engine, schema="public", table=table)
    if st2 == "OK":
        self.stdout.write(self.style.SUCCESS(msg2))
    else:
        self.stdout.write(self.style.WARNING(msg2))

    return ("OK", f"{path.name} -> {schema}.{table} (SRID={srid}, GEOM={geom_type}, rows={len(gdf)})")

def read_geospatialfile_ids_from_csv(csv_path: Path) -> list[int]:
    if not csv_path.exists():
        raise CommandError(f"CSV no existe: {csv_path}")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise CommandError("CSV sin encabezados (header). Debe tener 'id' o 'geospatialfile_id'.")

        # normalizamos nombres
        fields = {h.strip().lower(): h for h in reader.fieldnames}
        col = None
        if "geospatialfile_id" in fields:
            col = fields["geospatialfile_id"]
        elif "id" in fields:
            col = fields["id"]
        else:
            raise CommandError("CSV debe tener una columna 'id' o 'geospatialfile_id'.")

        ids = []
        for i, row in enumerate(reader, start=2):
            raw = (row.get(col) or "").strip()
            if not raw:
                continue
            try:
                ids.append(int(raw))
            except ValueError:
                raise CommandError(f"Valor no numérico en línea {i}: {col}='{raw}'")

    # unique preservando orden
    seen = set()
    out = []
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


class Command(BaseCommand):
    help = "Importa GeospatialFile(s) a PostGIS (crea tabla + migra atributos y geometrías)."

    def add_arguments(self, parser):
        parser.add_argument("--shp", required=True, help="Ruta al .zip")
        parser.add_argument("--schema", default="public", help="Schema destino (default: public)")
        parser.add_argument("--table-prefix", default='', help="Prefijo para tablas (default:'')")
        parser.add_argument("--if-exists", choices=["append", "replace"], default="append", help="append o replace")
        parser.add_argument("--target-srid", type=int, default=None, help="Reproyectar a este EPSG (opcional)")
        parser.add_argument("--out-sql", default='favex_sql', help="Carpeta donde guardar .sql (opcional)")
        parser.add_argument("--database", default="default", help="Alias en settings.DATABASES (default: default)")
        parser.add_argument("--no-postgis-extension", action="store_true", help="No intenta crear extensión postgis")

    def handle(self, *args, **options):
        shp = options["shp"]
        schema = options["schema"]
        table_prefix = options["table_prefix"]
        if_exists = options["if_exists"]
        target_srid = options["target_srid"]
        out_sql_dir = Path(options["out_sql"]) if options["out_sql"] else None
        db_alias = options["database"]
        

        db_url = sqlalchemy_url_from_django(db_alias)
        engine = create_engine(db_url, future=True)

        if not options["no_postgis_extension"]:
            ensure_postgis(engine)

        ok = 0
        skip = 0
        err = 0

        table_override = None
 
        status, msg = ingest_one(
            engine=engine,
            path=Path(shp),
            schema=schema,
            table_prefix=table_prefix,
            if_exists=if_exists,
            target_srid=target_srid,
            out_sql_dir=out_sql_dir,
            table_name_override=table_override,
        )

        if status == "OK":
            ok += 1
            self.stdout.write(self.style.SUCCESS(f"[GeospatialFile {shp}] {msg}"))
        else:
            skip += 1
            self.stdout.write(self.style.WARNING(f"[GeospatialFile {shp}] {msg}"))

        self.stdout.write(f"Resumen: OK={ok}, SKIP={skip}, ERROR={err}")
        if err:
            raise CommandError("Hubo errores durante la ingesta (ver logs arriba).")
