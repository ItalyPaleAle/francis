-- Preserve alarm_lease_id on live leases released by actor deactivation so only the interrupted execution can finalize its occurrence, while clearing expired lease IDs
CREATE OR REPLACE FUNCTION %sactive_actors_delete_update_alarms_fn()
RETURNS trigger AS $$
BEGIN
    UPDATE %salarms
    SET
        alarm_lease_id = CASE
            WHEN %salarms.alarm_lease_expiration_time IS NOT NULL
                AND %salarms.alarm_lease_expiration_time >= (now() AT TIME ZONE 'utc')
            THEN %salarms.alarm_lease_id
            ELSE NULL
        END,
        alarm_lease_expiration_time = NULL
    FROM old_rows r
    WHERE
        %salarms.actor_type = r.actor_type
        AND %salarms.actor_id = r.actor_id
        AND %salarms.alarm_lease_id IS NOT NULL;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
