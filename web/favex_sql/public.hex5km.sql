CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."hex5km" (
  "gid" BIGSERIAL PRIMARY KEY,
  "left" DOUBLE PRECISION,
  "top" DOUBLE PRECISION,
  "right" DOUBLE PRECISION,
  "bottom" DOUBLE PRECISION,
  "id_hex" BIGINT,
  "area_hex_k" DOUBLE PRECISION,
  "geom" geometry(GEOMETRY,4326)
);
CREATE INDEX IF NOT EXISTS "hex5km_geom_gix" ON "public"."hex5km" USING GIST ("geom");
