import json
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from django.contrib.gis.db.models.functions import AsGeoJSON
from django.db import connection
import os, hashlib

def index(request):
    return render(request, "index.html")

def map(request):
    return render(request, "map.html")

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

    formaciones = [
      "Bosque caducifolio andino del Bíobío",
      "Desierto del Tamarugal",
      "Matorral patagónico con Araucaria",
      "Bosque esclerófilo costero",
      "Desierto interior de Taltal",
      "Estepa altoandina desértica",
      "Matorral estepario boscoso",
      "Altas cumbres sin vegetación",
      "Bosque laurifolio andino",
      "Desierto de la cuenca superior del río",
      "Bosque caducifolio de Santiago",
      "Desierto costero de Tocopilla",
      "Desierto florido de los llanos",
      "Bosque siempreverde montano",
      "Bosque siempreverde andino",
      "Bosque caducifolio templado andino",
      "Bosque siempreverde templado costero",
      "Bosque esclerófilo interior",
      "Bosque siempreverde templado andino",
      "Bosque caducifolio templado",
      "Bosque caducifolio patagónico",
      "Bosque caducifolio patagónico costero",
      "Bosque siempreverde patagónico",
      "Bosque siempreverde patagónico costero",
      "Bosque siempreverde patagónico montano",
      "Bosque laurifolio templado costero",
      "Bosque laurifolio templado andino",
      "Matorral estepario costero",
      "Matorral estepario interior",
      "Matorral estepario andino",
      "Matorral estepario montano",
      "Matorral estepario altoandino",
      "Matorral estepario patagónico",
      "Matorral estepario patagónico costero",
      "Matorral estepario patagónico montano",
      "Matorral estepario magallánico",
      "Matorral estepario magallánico costero",
      "Matorral estepario magallánico montano",
      "Estepa patagónica",
      "Estepa patagónica costera",
      "Estepa patagónica montana",
      "Estepa magallánica",
      "Estepa magallánica costera",
      "Estepa magallánica montana",
      "Pradera patagónica",
      "Pradera patagónica costera",
      "Pradera patagónica montana",
      "Pradera magallánica",
      "Pradera magallánica costera",
      "Pradera magallánica montana",
      "Turberas patagónicas",
      "Turberas patagónicas costeras",
      "Turberas patagónicas montanas",
      "Turberas magallánicas",
      "Turberas magallánicas costeras",
      "Turberas magallánicas montanas",
      "Humedales altoandinos",
      "Humedales costeros",
      "Humedales interiores",
      "Humedales patagónicos",
      "Humedales magallánicos",
      "Salinas y salares",
      "Vegetación halófila costera",
      "Vegetación halófila interior",
      "Vegetación psamófila costera",
      "Vegetación psamófila interior",
      "Vegetación rupícola",
      "Vegetación de quebradas",
      "Vegetación de oasis",
      "Vegetación de vegas y bofedales",
      "Vegetación de mallines",
      "Vegetación de lagunas altoandinas",
      "Vegetación de riberas",
      "Vegetación de dunas costeras",
      "Vegetación de dunas interiores",
      "Matorral mixto y brezal turboso de Navar",
      "Estepa patagónica de Magallanes",
    ]


    match_expr = ["match", ["get", "formacion"]]
    for f in formaciones:
        match_expr.extend([f, _stable_hsl(f)])
    match_expr.append("#9e9e9e")  # default

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
              "paint":{"line-width":1, "line-opacity":0.4}
            },
            {
                "id": "formaciones-fill",
                "type": "fill",
                "source": "tegola",
                "source-layer": "formaciones",
                "layout": {"visibility": "none"},
                "paint": {
                    "fill-opacity": 0.8,
                    "fill-color": match_expr,
                },
            },

        ]
    }
    return JsonResponse(style)

def _stable_hsl(name: str) -> str:
    h = int(hashlib.md5(name.encode("utf-8")).hexdigest()[:8], 16) % 360
    return f"hsl({h}, 70%, 50%)"

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



