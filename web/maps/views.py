import json
from django.conf import settings
from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_GET
from django.contrib.gis.db.models.functions import AsGeoJSON
from django.db import connection
import os, hashlib
import io
from datetime import datetime
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.units import mm

import json

def index(request):
    return render(request, "index.html")

def documents(request):
    return render(request, "documents.html")

def about(request):
    return render(request, "about.html")

def map(request):
    return render(request, "map.html", {"GEOSERVER_BASE_URL": settings.GEOSERVER_BASE_URL})

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
              "paint":{"line-width":1, "line-opacity":0.4}
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
      SELECT hex_id, geom_3857
      FROM public.hex5km
      WHERE hex_id = %s
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

def _round3(v):
    try:
        if isinstance(v, (int, float)) and v == v:  # v==v filtra NaN
            return round(v, 3)
    except Exception:
        pass
    return v


@csrf_exempt 
@require_POST
def export_sr_pdf(request):
    # 1) parsea JSON
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"ok": False, "error": "JSON inválido"}, status=400)

    props = payload.get("props") or {}
    items = payload.get("items") or []

    # 2) saneo básico / límites
    if not isinstance(props, dict) or not isinstance(items, list):
        return JsonResponse({"ok": False, "error": "Formato inválido"}, status=400)

    items = items[:200]  # limita por seguridad

    # 3) arma PDF en memoria
    buffer = io.BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=16 * mm,
        leftMargin=16 * mm,
        topMargin=16 * mm,
        bottomMargin=16 * mm,
        title="Reporte SR",
    )

    styles = getSampleStyleSheet()
    story = []

    hex_id = props.get("hex_id") or "Sin ID"

    story.append(Paragraph("Reporte Índice Singularidad y Representatividad (SR)", styles["Title"]))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"<b>ID Conglomerado:</b> {hex_id}", styles["Normal"]))
    story.append(Paragraph(f"<b>Generado:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles["Normal"]))
    story.append(Spacer(1, 10))

    # Tabla SR
    fields = [
        ("ID Conglomerado", props.get("hex_id")),
        ("Categoría SR", props.get("sr_cat")),
        ("Valor SR", _round3(props.get("sr"))),
        ("Valor Rareza de Formación (RF)", _round3(props.get("rf"))),
        ("Valor Representatividad Ecosistémica (REP)", _round3(props.get("rep"))),
    ]
    fields = [(k, v) for (k, v) in fields if v not in (None, "", [])]

    story.append(Paragraph("<b>Índice SR</b>", styles["Heading2"]))
    table_data = [["Campo", "Valor"]] + [[str(k), str(v)] for k, v in fields]

    t = Table(table_data, colWidths=[80 * mm, 90 * mm])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f2f2f2")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#dddddd")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#fbfbfb")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 12))

    # Tabla Formaciones
    story.append(Paragraph("<b>Formaciones Vegetacionales</b>", styles["Heading2"]))

    if not items:
        story.append(Paragraph("Sin intersecciones con formaciones.", styles["Normal"]))
    else:
        rows = [["#", "Nombre", "Intersección (km²)"]]
        for i, x in enumerate(items, start=1):
            nombre = x.get("nombre") or x.get("name") or "Sin nombre"
            inter = x.get("inter_km2")
            inter_txt = ""
            try:
                if inter is not None:
                    inter_txt = f"{float(inter):.3f}"
            except Exception:
                inter_txt = str(inter) if inter is not None else ""

            rows.append([str(i), str(nombre), inter_txt])

        tf = Table(rows, colWidths=[10 * mm, 130 * mm, 30 * mm])
        tf.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f2f2f2")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#dddddd")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#fbfbfb")]),
                    ("ALIGN", (2, 1), (2, -1), "RIGHT"),
                ]
            )
        )
        story.append(tf)

    doc.build(story)

    pdf_bytes = buffer.getvalue()
    buffer.close()

    filename = f"hex_{hex_id}.pdf".replace(" ", "_")

    resp = HttpResponse(pdf_bytes, content_type="application/pdf")
    resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp

