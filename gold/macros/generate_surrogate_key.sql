{#
  Hand-rolled replacement for dbt_utils.generate_surrogate_key, implemented locally so
  this project has zero required network access at build time (`dbt build` works with
  no `dbt deps` step) -- see docs/adr/0003-duckdb-vs-bigquery-for-dbt.md for the broader
  "clone and run with no external dependencies" goal this project holds itself to.

  Concatenates the given column expressions with a delimiter unlikely to appear in the
  data, coalescing nulls to a sentinel so that a null component doesn't null out the
  whole hash (the same failure mode dbt_utils' version guards against).
#}

{% macro generate_surrogate_key(column_list) %}
    md5(
        {%- for col in column_list %}
        coalesce(cast({{ col }} as varchar), '_dbt_surrogate_key_null_')
        {%- if not loop.last %} || '||' || {% endif -%}
        {% endfor %}
    )
{% endmacro %}
