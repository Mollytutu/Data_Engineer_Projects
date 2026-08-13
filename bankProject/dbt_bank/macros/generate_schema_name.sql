{% macro generate_schema_name(custom_schema_name, node) -%}
    {# Use explicit layer schemas instead of dbt's default STAGING_MARTS form. #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
