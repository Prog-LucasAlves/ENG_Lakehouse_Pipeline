CREATE OR REFRESH STREAMING LIVE TABLE generoproduto
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  *,
  current_timestamp() AS ingestion_ts_utc
FROM STREAM(lakehouse.raw.generoproduto)