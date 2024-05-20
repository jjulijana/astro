UPDATE {{ params.schema }}.{{ params.table_name }}
SET movie_title = TRIM(TRAILING '\0' FROM movie_title);
