UPDATE {{ params.schema }}.{{ params.table_name }} SET plot_keywords = COALESCE(plot_keywords, 'none');
UPDATE {{ params.schema }}.{{ params.table_name }} SET content_rating = COALESCE(content_rating, 'Not Rated');

DO $$
DECLARE
    schema_name TEXT := '{{ params.schema }}';
    table__name TEXT := '{{ params.table_name }}';
    column_record RECORD;
BEGIN
    FOR column_record IN 
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = schema_name 
        AND table_name = table__name 
        AND column_name LIKE '%name' 
    LOOP
        EXECUTE 'UPDATE ' || schema_name || '.' || table__name || 
                ' SET ' || column_record.column_name || ' = COALESCE(' || column_record.column_name || ', ''unknown'')';
    END LOOP;
END $$;

DO $$
DECLARE
    col text;
BEGIN
    FOR col IN SELECT unnest(ARRAY['color', 'language', 'country', 'aspect_ratio'])
    LOOP
        EXECUTE format(
            'UPDATE %I.%I SET %I = COALESCE(%I, (SELECT %I FROM %I.%I WHERE %I IS NOT NULL GROUP BY %I ORDER BY COUNT(*) DESC LIMIT 1))',
            '{{ params.schema }}', '{{ params.table_name }}', col, col, col, '{{ params.schema }}', '{{ params.table_name }}', col, col
        );
    END LOOP;
END $$;

DO $$
DECLARE
    schema_name TEXT := '{{ params.schema }}';
    table__name TEXT := '{{ params.table_name }}';
    column_record RECORD;
    col TEXT;
BEGIN
    FOR column_record IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = schema_name
        AND table_name = table__name
        AND data_type IN ('integer', 'double precision', 'numeric')
    LOOP
        col := column_record.column_name;
        EXECUTE format(
            'UPDATE %I.%I SET %I = COALESCE(%I, (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY %I) FROM %I.%I))',
            schema_name, table__name, col, col, col, schema_name, table__name
        );
    END LOOP;
END $$;

