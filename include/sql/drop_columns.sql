{% for column in params.columns_to_drop %}
ALTER TABLE {{ params.schema }}.{{ params.table_name }} DROP COLUMN IF EXISTS {{ column }} CASCADE;
{% endfor %}
