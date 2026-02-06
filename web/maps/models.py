from django.contrib.gis.db import models as gis_models

class Lugar(gis_models.Model):
    nombre = gis_models.CharField(max_length=100)
    categoria = gis_models.CharField(max_length=50)
    geom = gis_models.PointField(srid=4326)

    def __str__(self):
        return self.nombre
