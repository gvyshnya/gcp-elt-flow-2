MERGE `{{params.project_name}}.lei_golden.Isins` final
USING (
SELECT LEI, ISIN from `{{params.project_name}}.lei_staging.Isins`
) staging
ON final.ISIN = staging.ISIN and final.LEI = staging.LEI
WHEN MATCHED THEN
UPDATE SET
final.ingest_date = final.ingest_date
WHEN NOT MATCHED THEN
INSERT (
LEI,
ISIN,
ingest_date
) VALUES (
staging.LEI,
staging.ISIN,
CURRENT_DATETIME()
)