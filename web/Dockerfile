FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencias del sistema para GeoDjango
RUN apt-get update && apt-get install -y \
    binutils \
    libproj-dev \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# Variables para que GeoDjango encuentre GDAL
ENV GDAL_LIBRARY_PATH=/usr/lib/libgdal.so
ENV PROJ_LIB=/usr/share/proj

COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/
