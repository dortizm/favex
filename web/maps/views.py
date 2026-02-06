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
            }
        },
        "layers": [
            {"id": "osm-base", "type": "raster", "source": "osm"},
            { "id":"hex5km-line", "type":"line", "source":"tegola", "source-layer":"hex5km",
              "paint":{"line-width":1, "line-opacity":0.7}
            }

        ]
    }
    return JsonResponse(style)

