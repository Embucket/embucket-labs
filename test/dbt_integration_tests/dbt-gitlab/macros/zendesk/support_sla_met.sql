{% macro support_sla_met(first_reply_time, ticket_priority, created_at) %}
 
    {% set day_after_created = dbt_utils.dateadd(datepart='day', interval=1, from_date_or_timestamp=created_at) %}
    {% set minutes_before_day_end = dbt_utils.datediff(created_at, day_after_created, 'mins') %}

    CASE
      -- Logic for urgent tickets with a 24/7 SLA of 30 minutes
      WHEN {{ first_reply_time }} <= 30
          AND {{ ticket_priority }} = 'urgent'
        THEN True
      -- Logic for high priority tickets with a 24/5 SLA of 4 hours or 240 minutes
      WHEN {{ first_reply_time }} <= 240
          AND {{ ticket_priority }} = 'high'
        THEN True
      -- Logic for tickets submitted on a Friday after 8 pm.
      -- Minutes remaining in day + minutes elapsed over the weekend (2880) + unused SLA minutes
      WHEN {{ ticket_priority }} = 'high'
          AND DAYOFWEEK({{ created_at }}) = 5
          AND HOUR( {{ created_at }} ) >= 20
          AND {{ first_reply_time }} <=
            {{ minutes_before_day_end }} + 2880 + (240 - {{ minutes_before_day_end }})
        THEN True
      -- Logic for high priority tickets submitted over the weekend. Minutes elapsed over the weekend (2880) + allotted SLA minutes
      WHEN {{ ticket_priority }} = 'high'
          AND DAYOFWEEK({{ created_at }}) = 6
          OR DAYOFWEEK({{ created_at }}) = 0
          AND {{ first_reply_time }} <= 2880 + 240
        THEN True
      -- Logic for normal priority tickets with a 24/5 SLA of 8 hours or 480 minutes
      WHEN {{ first_reply_time }} <= 480
          AND {{ ticket_priority }} = 'normal'
        THEN True
      -- Logic for normal priority tickets submitted on a Friday after 4 pm. Minutes remaining in day +  minutes elapsed over the weekend (2880) + Unused SLA minutes
      WHEN {{ ticket_priority }} = 'normal'
          AND DAYOFWEEK({{ created_at }}) = 5
          AND HOUR( {{ created_at }} ) >= 16
          AND {{ first_reply_time }} <=
            {{ minutes_before_day_end }} + 2880 + (480 - {{ minutes_before_day_end }})
        THEN True
      -- Logic for normal priority tickets submitted over the weekend. Minutes elapsed over the weekend + allotted SLA minutes
      WHEN {{ ticket_priority }} = 'normal'
          AND DAYOFWEEK({{ created_at }}) = 6
          OR DAYOFWEEK({{ created_at }}) = 0
          AND {{ first_reply_time }} <= 2880 + 480
        THEN True
      -- Logic for low priority tickets with a 24/5 SLA of 24 hours or 1440 minutes
      WHEN {{ first_reply_time }} <= 1440
          AND {{ ticket_priority }} = 'low'
        THEN True
      -- Logic for low priority tickets submitted on a Friday after 12am. Minutes remaining in day + minutes elapsed over the weekend (2880) + unused SLA minutes
      WHEN {{ ticket_priority }} = 'low'
          AND DAYOFWEEK({{ created_at }}) = 5
          AND HOUR( {{ created_at }} ) > 0
          AND {{ first_reply_time }} <=
            {{ minutes_before_day_end }} + 2880 + (1440 - {{ minutes_before_day_end }})
        THEN True
      -- Logic for low priority tickets submitted over the weekend. Minutes elapsed over the weekend (2880) + allotted SLA minutes
      WHEN {{ ticket_priority }} = 'low'
          AND DAYOFWEEK({{ created_at }}) = 6
          OR DAYOFWEEK({{ created_at }}) = 0
          AND {{ first_reply_time }} <= 2880 + 1440
        THEN True

      ELSE False

      END

{% endmacro %}
