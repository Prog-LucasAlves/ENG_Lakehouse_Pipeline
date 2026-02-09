CREATE OR REFRESH STREAMING LIVE TABLE clientes
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  *,
  current_timestamp() AS ingestion_ts_utc
FROM STREAM(lakehouse.raw.clientes)