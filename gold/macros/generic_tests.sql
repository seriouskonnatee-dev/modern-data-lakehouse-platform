{#
  Hand-rolled replacements for the two dbt_utils generic tests used in this project's
  schema.yml files (accepted_range), again to avoid a hard dependency on `dbt deps`
  succeeding (no network access required to build this project -- see
  docs/adr/0003-duckdb-vs-bigquery-for-dbt.md).
#}

{% test accepted_range(model, column_name, min_value=none, max_value=none) %}

    select *
    from {{ model }}
    where
        {{ column_name }} is not null
        and (
            {% if min_value is not none %} {{ column_name }} < {{ min_value }} {% endif %}
            {% if min_value is not none and max_value is not none %} or {% endif %}
            {% if max_value is not none %} {{ column_name }} > {{ max_value }} {% endif %}
        )

{% endtest %}
