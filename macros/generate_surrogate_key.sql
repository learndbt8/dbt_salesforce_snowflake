{% macro generate_surrogate_key(field_list) %}
    {%- set fields = [] -%}
    {%- for field in field_list -%}
        {%- do fields.append(
            "coalesce(cast(" ~ field ~ " as " ~ dbt.type_string() ~ "), '_dbt_utils_surrogate_key_null_')"
        ) -%}
        {%- if not loop.last %}
            {%- do fields.append("'-'") -%}
        {%- endif -%}
    {%- endfor -%}
    {{ dbt.hash(dbt.concat(fields)) }}
{% endmacro %}