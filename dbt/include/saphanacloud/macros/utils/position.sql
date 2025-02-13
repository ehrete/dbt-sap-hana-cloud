{% macro saphanacloud__position(substring_text, string_text) %}

    LOCATE(
       {{ string_text }} , {{ substring_text }}
    )

{%- endmacro -%}