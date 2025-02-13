{% macro saphanacloud__hash(field) -%}
    TO_VARCHAR(HASH_MD5 (TO_BINARY(cast({{ field }} as NVARCHAR))))
{%- endmacro %}