DROP TABLE IF EXISTS {{ params.schema }}.{{ params.target_table }};

CREATE TABLE {{ params.schema }}.{{ params.target_table }} AS
SELECT DISTINCT *
FROM {{ params.schema }}.{{ params.source_table }}
WHERE 1=0;

INSERT INTO {{ params.schema }}.{{ params.target_table }}
SELECT DISTINCT *
FROM {{ params.schema }}.{{ params.source_table }};
