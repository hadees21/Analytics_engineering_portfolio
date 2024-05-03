{{ config(materialized = 'incremental', unique_key = 'unique_ticket_purchase_key') }}

WITH seq_of_seats AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY NULL) AS value
    FROM TABLE(GENERATOR(ROWCOUNT => 1000))
),
tm_ticket_exchange_processed AS (
    SELECT
        {{ dbt_utils.star(from=source('ticketmaster', 'tm_ticket_exchange'), relation_alias='te') }},
        te.first_seat_num + s.value - 1 AS seat_num
    FROM {{ source('ticketmaster', 'tm_ticket_exchange') }} te
    JOIN seq_of_seats s ON s.value <= num_seats
),
tm_tickets_processed AS (
    SELECT
        ticket_exchange_id,
        CONCAT(ticket_exchange_id, '-', SEAT_NUM) AS unique_ticket_purchase_key,
        CONCAT(SEAT_SECTION_ID, '-', SEAT_ROW_NUM, '-', SEAT_NUM) AS unique_seat_key,
        event_id,
        account_id,
        seat_section_id,
        seat_row_num,
        first_seat_num,
        num_seats,
        activity_date,
        rep_email,
        total_ticket_price / num_seats AS price_of_ticket,
        order_num,
        seat_num
    FROM tm_ticket_exchange_processed
),
tickets AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_id, unique_seat_key ORDER BY activity_date ASC) AS ticket_transaction_order,
        ROW_NUMBER() OVER (PARTITION BY event_id, unique_seat_key ORDER BY activity_date DESC) AS ticket_transaction_order_reversed
    FROM tm_tickets_processed
),
tm_event_processed AS (
    SELECT
        {{ dbt_utils.star(from=source('ticketmaster', 'tm_event')) }}
    FROM {{ source('ticketmaster', 'tm_event') }}
),
crm_contact_processed AS (
    SELECT
        {{ dbt_utils.star(from=source('crm', 'crm_contact')) }}
    FROM {{ source('crm', 'crm_contact') }}
),
merged AS (
    SELECT
        tme.*,
        te.season_id,
        te.event_date_start,
        te.event_name,
        cc.contact_id,
        cc.first_name,
        cc.last_name,
        cc.email,
        cc.mailing_address,
        cc.mailing_city,
        cc.mailing_state,
        cc.mailing_zip,
        cc.is_season_ticket_holder,
        cc.is_broker,
        LAG(tme.price_of_ticket, 1, tme.price_of_ticket) OVER (PARTITION BY tme.event_id, tme.seat_unique_key ORDER BY tme.activity_date) AS previous_price_of_ticket
    FROM tickets tme
    JOIN tm_event_processed te ON tme.event_id = te.event_id
    JOIN crm_contact_processed cc ON tme.account_id = cc.tm_account_id
),
final AS (
    SELECT
        *,
        CASE WHEN activity_date = event_date_start THEN 1 ELSE 0 END AS is_game_day_transaction,
        CASE WHEN ticket_transaction_order = 1 THEN 1 ELSE 0 END AS is_original_ticket_holder,
        CASE WHEN ticket_transaction_order_reversed = 1 THEN 1 ELSE 0 END AS is_latest_ticket_holder,
        CASE WHEN {{ is_local_buyer_sql('mailing_city') }} THEN 1 ELSE 0 END AS is_local_attendee,
        price_of_ticket - previous_price_of_ticket AS price_difference_paid
    FROM merged
)

SELECT
    *,
    '{{ invocation_id }}' AS run_id,
    CURRENT_TIMESTAMP AS record_updated
FROM final
{% if is_incremental() %}
    -- Only select rows where the activity date is newer than the latest in the existing model and is the latest ticket holder
    WHERE activity_date > (SELECT MAX(activity_date) FROM {{ this }})
      AND is_latest_ticket_holder = 1
{% else %}
    -- For full refresh, select only rows where is the latest ticket holder
    WHERE is_latest_ticket_holder = 1
{% endif %}


-- {% macro is_local_buyer_sql(city_column) %}
--     {% set local_cities = "'Chicago', 'Aurora', 'Naperville'" %}  
--     {{ city_column }} IN ({{ local_cities }})
-- {% endmacro %}