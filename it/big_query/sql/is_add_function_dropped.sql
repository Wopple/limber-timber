SELECT COUNT(*) = 0
FROM `{schema}.INFORMATION_SCHEMA.ROUTINES`
WHERE
    routine_name = 'add'
    AND routine_type = 'FUNCTION'
