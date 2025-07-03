with src_host as (
select * from {{ ref('src_hosting') }}
)
select
HOST_ID,
	NVL(
        host_name,
        'Anonymous'
    ) AS host_name,
	IS_SUPERHOST,
	CREATED_AT,
	UPDATED_AT
FROM
    src_host