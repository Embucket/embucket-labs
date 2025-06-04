{%- macro zuora_slugify(input_text) -%}

    trim(
        lower(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        {{ input_text }}
                    , '\\s+\\|{2}\\s+', '|')
                , '[ ]{2,}', ' ')
            , '[^A-Za-z0-9|]', '-')
            )
        )

{%- endmacro -%}
