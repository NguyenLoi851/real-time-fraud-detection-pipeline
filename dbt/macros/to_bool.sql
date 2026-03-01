{% macro to_bool(value_expression) %}
case
  when lower(trim(cast({{ value_expression }} as string))) in ('true', 't', '1', 'yes', 'y') then true
  when lower(trim(cast({{ value_expression }} as string))) in ('false', 'f', '0', 'no', 'n') then false
  else null
end
{% endmacro %}