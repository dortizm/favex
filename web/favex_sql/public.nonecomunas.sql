CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."nonecomunas" (
  "gid" BIGSERIAL PRIMARY KEY,
  "cut_reg" TEXT,
  "cut_prov" TEXT,
  "cut_com" TEXT,
  "region" TEXT,
  "provincia" TEXT,
  "comuna" TEXT,
  "superficie" DOUBLE PRECISION,
  "geom" geometry(GEOMETRY,4326)
);
CREATE INDEX IF NOT EXISTS "nonecomunas_geom_gix" ON "public"."nonecomunas" USING GIST ("geom");
