import json
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from django.contrib.gis.db.models.functions import AsGeoJSON
from .models import Lugar
import os


def mapa(request):
    categorias = (
        Lugar.objects.values_list("categoria", flat=True)
        .distinct()
        .order_by("categoria")
    )
    return render(request, "maps/map.html", {"categorias": categorias})


@require_GET
def lugares_geojson(request):
    categoria = request.GET.get("categoria")
    qs = Lugar.objects.all()
    if categoria:
        qs = qs.filter(categoria=categoria)

    features = []
    for lugar in qs.annotate(geojson=AsGeoJSON("geom")):
        geometry = json.loads(lugar.geojson)
        features.append(
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "id": lugar.id,
                    "nombre": lugar.nombre,
                    "categoria": lugar.categoria,
                },
            }
        )

    data = {
        "type": "FeatureCollection",
        "features": features,
    }
    return JsonResponse(data)

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

