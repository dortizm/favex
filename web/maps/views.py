import json
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from django.contrib.gis.db.models.functions import AsGeoJSON
from django.db import connection
import os

def mapa(request):
    return render(request, "maps/map.html")

def regions(request):
    with connection.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT cut_reg, region
            FROM comunas
            WHERE cut_reg IS NOT NULL
              AND region IS NOT NULL AND region <> ''
            ORDER BY region
        """)
        data = [{"code": r[0], "name": r[1]} for r in cur.fetchall()]
    return JsonResponse(data, safe=False)

def provinces(request):
    cut_reg = request.GET.get("cut_reg", "")
    with connection.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT cut_prov, provincia
            FROM comunas
            WHERE cut_reg = %s
              AND cut_prov IS NOT NULL
              AND provincia IS NOT NULL AND provincia <> ''
            ORDER BY provincia
        """, [cut_reg])
        data = [{"code": r[0], "name": r[1]} for r in cur.fetchall()]
    return JsonResponse(data, safe=False)

def communes(request):
    cut_reg = request.GET.get("cut_reg", "")
    cut_prov = request.GET.get("cut_prov", "")
    with connection.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT cut_com, comuna
            FROM comunas
            WHERE cut_reg = %s
              AND cut_prov = %s
              AND cut_com IS NOT NULL
              AND comuna IS NOT NULL AND comuna <> ''
            ORDER BY comuna
        """, [cut_reg, cut_prov])
        data = [{"code": r[0], "name": r[1]} for r in cur.fetchall()]
    return JsonResponse(data, safe=False)

def mvt_style(request):
    tegola_public = os.environ.get("TEGOLA_PUBLIC_URL", "http://localhost:9090")
    map_name = os.environ.get("TEGOLA_MAP_NAME", "base")
    layer_name = os.environ.get("TEGOLA_LAYER_NAME", "hex5km")

    style = {
        "version": 8,
        "sources": {
            "osm": {
              "type": "raster",
              "tiles": ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
              "tileSize": 256,
              "attribution": "© OpenStreetMap contributors"
            },
            "tegola": {
                "type": "vector",
                "tiles": [f"{tegola_public}/maps/{map_name}/{{z}}/{{x}}/{{y}}.pbf"],
                "minzoom": 0,
                "maxzoom": 14,
            },
        },
        "layers": [
            {"id": "osm-base", "type": "raster", "source": "osm"},
            { "id":"hex5km-line", "type":"line", "source":"tegola", "source-layer":"hex5km",
              "paint":{"line-width":1, "line-opacity":0.7}
            },
            {
              "id":"formaciones-line",
              "type":"fill",
              "source":"tegola",
              "source-layer":"formaciones",
              "layout": {"visibility": "none"},
              "paint":{
                "fill-opacity": 0.25,
                "fill-color": "#2e7d32"
              }
            },

        ]
    }
    return JsonResponse(style)

@require_GET
def hex_formaciones(request):
    """
    GET /api/hex-formaciones/?id_hex=123
    Retorna lista de formaciones que intersectan el hex, ordenadas por área de intersección (km2).
    """
    id_hex = request.GET.get("id_hex")
    if not id_hex:
        return JsonResponse({"ok": False, "error": "Falta parámetro id_hex"}, status=400)

    # Ajusta nombres de campos según tu esquema:
    # - hex5km: geom_3857, id_hex
    # - formaciones: geom_3857, gid, nombre (o el campo que represente el nombre)
    sql = """
    WITH h AS (
      SELECT id_hex, geom_3857
      FROM public.hex5km
      WHERE id_hex = %s
      LIMIT 1
    )
    SELECT
      f.gid AS id,
      f.formacion AS nombre,
      ST_Area(ST_Intersection(f.geom_3857, h.geom_3857)) / 1000000.0 AS inter_km2
    FROM public.formaciones f
    JOIN h ON (f.geom_3857 && h.geom_3857)
    WHERE ST_Intersects(f.geom_3857, h.geom_3857)
    ORDER BY inter_km2 DESC;
    """

    with connection.cursor() as cur:
        cur.execute(sql, [id_hex])
        rows = cur.fetchall()

    data = [{"id": r[0], "nombre": r[1], "inter_km2": float(r[2]) if r[2] is not None else 0.0} for r in rows]
    return JsonResponse({"ok": True, "id_hex": id_hex, "count": len(data), "items": data})
