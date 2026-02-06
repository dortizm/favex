from django.urls import path
from . import views

urlpatterns = [
    path("", views.mapa, name="mapa"),
    path("mvt/style.json", views.mvt_style, name="mvt_style"),
    path("api/regions/", views.regions),
    path("api/provinces/", views.provinces),
    path("api/communes/", views.communes),
]
