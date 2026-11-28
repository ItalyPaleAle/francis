-- Preserve alarm_lease_id on live leases cleared by actor deactivation so only the interrupted execution can finalize its occurrence, while clearing expired lease IDs
DROP TRIGGER %sactive_actors_delete_update_alarms;

CREATE TRIGGER %sactive_actors_delete_update_alarms
AFTER DELETE ON %sactive_actors
BEGIN
    UPDATE %salarms
    SET
        alarm_lease_id = CASE
            WHEN alarm_lease_expiration_time >= CAST(unixepoch('subsec') * 1000 AS INTEGER) THEN alarm_lease_id
            ELSE NULL
        END,
        alarm_lease_expiration_time = NULL
    WHERE
        actor_type = OLD.actor_type
        AND actor_id = OLD.actor_id
        AND alarm_lease_id IS NOT NULL
        AND alarm_lease_expiration_time IS NOT NULL;
END;
