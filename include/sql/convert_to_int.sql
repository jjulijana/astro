DO $$
DECLARE
    schema_name TEXT := '{{ params.schema }}';
    table__name TEXT := '{{ params.table_name }}';
    column_record RECORD;
    result INT;
BEGIN
    FOR column_record IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = schema_name
        AND table_name = table__name
        AND data_type IN ('numeric', 'double precision')
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM %I.%I WHERE %I <> floor(%I)',
            schema_name, table__name, column_record.column_name, column_record.column_name
        ) INTO result;

        IF result = 0 THEN
            EXECUTE format(
                'ALTER TABLE %I.%I ALTER COLUMN %I TYPE bigint USING %I::bigint',
                schema_name, table__name, column_record.column_name, column_record.column_name
            );
        END IF;
    END LOOP;
END $$;
