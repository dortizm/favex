from django.urls import path
from . import views

urlpatterns = [
    path("", views.mapa, name="mapa"),
    path("api/lugares/", views.lugares_geojson, name="lugares_geojson"),
    path("mvt/style.json", views.mvt_style, name="mvt_style"),
]
