-- TimescaleDB schema for the Barcelona accidents data.
-- Safe to run repeatedly: every statement is IF NOT EXISTS or guarded.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS accidents (
    numero_expedient          text             NOT NULL,
    -- Local Barcelona time, stored as timestamptz. The loader sets the session
    -- time zone to Europe/Madrid so Postgres applies the right UTC offset (incl. DST).
    accident_datetime         timestamptz      NOT NULL,
    accident_date             date             NOT NULL,
    year                      integer          NOT NULL,
    month                     integer          NOT NULL,
    day                       integer          NOT NULL,
    hour                      integer          NOT NULL,
    codi_districte            integer,
    nom_districte             text,
    codi_barri                integer,
    nom_barri                 text,
    codi_carrer               integer,
    nom_carrer                text,
    num_postal                text,
    descripcio_dia_setmana    text,
    nom_mes                   text,
    descripcio_torn           text,
    descripcio_causa_vianant  text,
    numero_morts              integer          NOT NULL CHECK (numero_morts >= 0),
    numero_lesionats_lleus    integer          NOT NULL CHECK (numero_lesionats_lleus >= 0),
    numero_lesionats_greus    integer          NOT NULL CHECK (numero_lesionats_greus >= 0),
    numero_victimes           integer          NOT NULL CHECK (numero_victimes >= 0),
    numero_vehicles_implicats integer          NOT NULL CHECK (numero_vehicles_implicats >= 0),
    utm_x_ed50                double precision,
    utm_y_ed50                double precision,
    longitude                 double precision NOT NULL,
    latitude                  double precision NOT NULL,
    source_file               text             NOT NULL,
    -- A unique constraint on a hypertable must include the partitioning column,
    -- so the case number alone cannot be the primary key.
    PRIMARY KEY (numero_expedient, accident_datetime)
);

-- Turn the table into a hypertable partitioned by time.
-- Chunk interval chosen by measurement on this data (~7.7k rows/year):
--   1 month -> 62 chunks, 21.7 MB -> 12.1 MB compressed (1.8x)
--   1 year  ->  6 chunks, 16.8 MB ->  3.4 MB compressed (4.9x)
-- Each chunk has fixed overhead (its own indexes and catalog entries), so tiny
-- chunks waste space. A year of data is ~3 MB, far below the "recent chunk fits
-- in ~25% of memory" guideline. Intervals are fixed widths, not calendar periods.
SELECT create_hypertable(
    'accidents',
    by_range('accident_datetime', INTERVAL '1 year'),
    if_not_exists => TRUE
);
-- Applies to an existing hypertable too (affects new chunks; the loader reloads all).
SELECT set_chunk_time_interval('accidents', INTERVAL '1 year');

-- Common access path: one district over a time range.
CREATE INDEX IF NOT EXISTS accidents_district_time_idx
    ON accidents (nom_districte, accident_datetime DESC);

-- Columnstore (Hypercore) compression. segmentby groups rows by district inside
-- each chunk (~700 rows per district per yearly chunk, enough to compress well);
-- orderby keeps them in time order so range filters can skip whole batches.
-- Settings cannot change while chunks are compressed, hence the guard.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM timescaledb_information.hypertables
        WHERE hypertable_name = 'accidents' AND compression_enabled
    ) THEN
        ALTER TABLE accidents SET (
            timescaledb.enable_columnstore = true,
            timescaledb.segmentby = 'nom_districte',
            timescaledb.orderby = 'accident_datetime DESC'
        );
    END IF;
END $$;

-- In production, chunks older than 3 months move to the columnstore automatically.
CALL add_columnstore_policy('accidents', after => INTERVAL '3 months', if_not_exists => true);

-- Continuous aggregate: daily totals per district, bucketed in Barcelona time.
CREATE MATERIALIZED VIEW IF NOT EXISTS accidents_daily_district
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 day', accident_datetime, 'Europe/Madrid') AS bucket,
    nom_districte,
    count(*)                    AS accidents,
    sum(numero_victimes)        AS victims,
    sum(numero_lesionats_lleus) AS minor_injuries,
    sum(numero_lesionats_greus) AS serious_injuries,
    sum(numero_morts)           AS deaths
FROM accidents
GROUP BY bucket, nom_districte
WITH NO DATA;

-- Keeps the recent window fresh. Historical loads are refreshed explicitly by the loader.
SELECT add_continuous_aggregate_policy(
    'accidents_daily_district',
    start_offset      => INTERVAL '3 months',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists     => true
);
