{{ config(
    materialized = 'view',
    tags = ['bronze'],
) }}

-- Modelo de ejemplo para la capa bronze
-- En la capa bronze no hacemos transformaciones complejas,
-- sólo extraemos los datos necesarios de la fuente

WITH source_data AS (
    SELECT
        id,
        created_at,
        updated_at,
        'sistema_a' AS source_system
    FROM 
        `{{ var('gcp_project') }}.raw_data.example_source`
    WHERE 
        _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
)

SELECT
    id,
    created_at,
    updated_at,
    source_system
FROM 
    source_data 