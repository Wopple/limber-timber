CREATE OR REPLACE FUNCTION `{schema}.add`(a INT64, b INT64, default_value INT64) RETURNS INT64 AS (
    COALESCE(a + b, default_value)
)
