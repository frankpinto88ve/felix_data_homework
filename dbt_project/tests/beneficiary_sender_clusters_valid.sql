SELECT *
FROM {{ ref('beneficiary_sender_clusters') }}
WHERE sending_user_count < 2
   OR ARRAY_LENGTH(sending_user_ids) <> sending_user_count
