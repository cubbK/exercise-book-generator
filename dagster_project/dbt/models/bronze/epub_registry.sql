{
{
    config
(
        materialized='incremental',
        unique_key='sha256_hash',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    file_id,
    filename,
    storage_path,
    sha256_hash,
    uploaded_at,
    status
FROM {{ source
('bronze_raw', 'epub_registry_raw') }}

{%
if is_incremental() %}
  WHERE sha256_hash NOT IN
(SELECT sha256_hash
FROM {{ this }})
{% endif %}
