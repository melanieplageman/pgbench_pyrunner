CREATE OR REPLACE FUNCTION freeze_errs(TEXT name) RETURNS TABLE
(
  name TEXT,
  members INT,
  vm_page_freezes INT,
  freezes INT,
  unfreezes INT,
  early_unfreezes INT,
  pcnt_error FLOAT,
  pcnt_freezes_attempted FLOAT,
  avg_frz_duration TEXT,
  avg_frz_page_age TEXT
)
BEGIN ATOMIC
SELECT
$1,
members,
vm_page_freezes,
freezes,
unfreezes,
early_unfreezes,
CASE vm_page_freezes
  WHEN 0 THEN NULL
  ELSE (early_unfreezes/vm_page_freezes::float * 100)::int
END AS pcnt_error,
CASE npages_start
  WHEN 0 THEN NULL
  ELSE (freezes/npages_start::float * 100)::int
END AS pcnt_freezes_attempted,
CASE unfreezes
  WHEN 0 THEN NULL
  ELSE pg_size_pretty(avg_frozen_duration_lsns::numeric)
END AS avg_frz_duration,
CASE freezes
  WHEN 0 THEN NULL
  ELSE pg_size_pretty(avg_page_age_lsns::numeric)
END AS avg_frz_page_age
FROM pg_stat_get_table_vacuums($1::regclass) ORDER BY start_lsn;
END;

CREATE OR REPLACE FUNCTION vacstats(TEXT name) RETURNS TABLE
(
  name TEXT,
  members INT,
  npages_end BIGINT,
  pcnt_frozen_end FLOAT,
  pcnt_scanned FLOAT,
  freeze_fpis BIGINT
)
BEGIN ATOMIC
SELECT
$1,
members,
npages_end,
CASE npages_end
  WHEN 0 THEN NULL
  ELSE (nfrozen_end/npages_end::float * 100)::int
END AS pcnt_frozen_end,
CASE npages_end
  WHEN 0 THEN NULL
  ELSE (scanned_pages/npages_end::float * 100)::int
END AS pcnt_scanned,
freeze_fpis
FROM pg_stat_get_table_vacuums($1::regclass) ORDER BY start_lsn;
END;

CREATE OR REPLACE FUNCTION av_efficacy(TEXT name) RETURNS TABLE
(
  name TEXT,
  members INT,
  set_av INT,
  unset_av INT,
  missed_freezes INT,
  page_age_threshold TEXT,
  avg_page_age TEXT,
  stddev_page_age TEXT,
  avg_av_duration TEXT,
  stddev_av_duration TEXT
)
BEGIN ATOMIC
SELECT
$1,
members,
av AS set_av,
unav AS unset_av,
missed_freezes,
pg_size_pretty(page_age_threshold::numeric) AS page_age_threshold,
pg_size_pretty(avg_av_page_age::numeric) AS avg_page_age,
pg_size_pretty(stddev_av_page_age::numeric) AS stddev_page_age,
pg_size_pretty(avg_av_dur::numeric) AS avg_av_duration,
pg_size_pretty(stddev_av_dur::numeric) AS stddev_av_duration
FROM pg_stat_get_table_vacuums($1::regclass) ORDER BY start_lsn;
END;
