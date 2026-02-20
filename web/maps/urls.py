from django.urls import path
from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("map", views.map, name="map"),
    path("mvt/style.json", views.mvt_style, name="mvt_style"),
    path("api/regions/", views.regions),
    path("api/provinces/", views.provinces),
    path("api/communes/", views.communes),
    path("api/hex-formaciones/", views.hex_formaciones, name="hex_formaciones"),
]
