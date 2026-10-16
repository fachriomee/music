{#
================================================================================
 Custom generic tests (no external packages needed => `dbt deps` not required,
 builds stay reproducible offline). A generic test returns the FAILING rows;
 zero rows = pass.
================================================================================
#}

{# Column must be a well-formed ISRC: CC XXX YY NNNNN (12 chars, no dashes) #}
{% test valid_isrc(model, column_name) %}
select {{ column_name }} as failing_value
from {{ model }}
where {{ column_name }} is not null
  and not match({{ column_name }}, '{{ isrc_regex() }}')
{% endtest %}


{# Column must match an arbitrary RE2 regular expression #}
{% test matches_regex(model, column_name, regex) %}
select {{ column_name }} as failing_value
from {{ model }}
where {{ column_name }} is not null
  and not match(toString({{ column_name }}), '{{ regex }}')
{% endtest %}


{# Combination of columns must be unique (grain test for bridge tables) #}
{% test unique_combination_of_columns(model, combination_of_columns) %}
select {{ combination_of_columns | join(', ') }}, count() as n
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having n > 1
{% endtest %}


{# Arbitrary boolean expression must hold for every row #}
{% test expression_is_true(model, expression, column_name=None) %}
select *
from {{ model }}
where not ({{ expression }})
{% endtest %}


{# Dates must not be in the future (1 day tolerance for timezones) #}
{% test not_in_future(model, column_name) %}
select {{ column_name }} as failing_value
from {{ model }}
where {{ column_name }} > today() + 1
{% endtest %}


{# Table must contain at least `min_rows` rows (catches silently empty builds) #}
{% test min_row_count(model, min_rows) %}
select count() as row_count
from {{ model }}
having row_count < {{ min_rows }}
{% endtest %}


{# Table must contain at most `max_rows` rows (e.g. quarantine size budget) #}
{% test max_row_count(model, max_rows) %}
select count() as row_count
from {{ model }}
having row_count > {{ max_rows }}
{% endtest %}
