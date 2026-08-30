-- Atomically stores one alarm and leases it when its actor can run on an allowed host
CREATE OR REPLACE FUNCTION %sset_and_lease_alarm_v1(
    p_alarm_id uuid,
    p_actor_type text,
    p_actor_id text,
    p_alarm_name text,
    p_alarm_due_time timestamp,
    p_alarm_interval text,
    p_alarm_ttl_time timestamp,
    p_alarm_data bytea,
    p_host_ids uuid[],
    p_host_health_check_deadline interval,
    p_alarms_fetch_ahead_interval interval,
    p_alarms_lease_duration interval
)
RETURNS TABLE (
    r_alarm_id uuid,
    r_alarm_due_time timestamp,
    r_lease_id uuid
) AS $$
DECLARE
    v_now timestamp;
    v_alarm_id uuid;
    v_alarm_due_time timestamp;
    v_actor_was_active boolean;
    v_actor_lock_key bigint;
BEGIN
    -- Store or replace the alarm while preserving a lease when the properties are identical
    INSERT INTO %salarms
        (alarm_id, actor_type, actor_id, alarm_name,
        alarm_due_time, alarm_interval, alarm_ttl_time, alarm_data,
        alarm_lease_id, alarm_lease_expiration_time)
    VALUES
        (p_alarm_id, p_actor_type, p_actor_id, p_alarm_name,
        p_alarm_due_time, p_alarm_interval, p_alarm_ttl_time, p_alarm_data,
        NULL, NULL)
    ON CONFLICT (actor_type, actor_id, alarm_name) DO UPDATE SET
        alarm_id = EXCLUDED.alarm_id,
        alarm_due_time = EXCLUDED.alarm_due_time,
        alarm_interval = EXCLUDED.alarm_interval,
        alarm_ttl_time = EXCLUDED.alarm_ttl_time,
        alarm_data = EXCLUDED.alarm_data,
        alarm_lease_id = NULL,
        alarm_lease_expiration_time = NULL
    WHERE
        %salarms.alarm_due_time != EXCLUDED.alarm_due_time
        OR %salarms.alarm_interval IS DISTINCT FROM EXCLUDED.alarm_interval
        OR %salarms.alarm_ttl_time IS DISTINCT FROM EXCLUDED.alarm_ttl_time
        OR %salarms.alarm_data IS DISTINCT FROM EXCLUDED.alarm_data;

    -- Lock the stored row only when it is within fetch-ahead and available for leasing
    v_now := now() AT TIME ZONE 'utc';
    SELECT alarm_id, alarm_due_time
    INTO v_alarm_id, v_alarm_due_time
    FROM %salarms
    WHERE
        actor_type = p_actor_type
        AND actor_id = p_actor_id
        AND alarm_name = p_alarm_name
        AND alarm_due_time <= v_now + p_alarms_fetch_ahead_interval
        AND (
            alarm_lease_id IS NULL
            OR alarm_lease_expiration_time IS NULL
            OR alarm_lease_expiration_time < v_now
        )
    FOR UPDATE;

    -- A missing result means the stored alarm is outside fetch-ahead or already has a live lease
    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Avoid waiting for an actor lock while holding the alarm row because the batch fetcher acquires those locks in the opposite order
    v_actor_was_active := %sactor_active_v1(
        p_actor_type,
        p_actor_id,
        v_now - p_host_health_check_deadline
    );
    IF NOT v_actor_was_active THEN
        v_actor_lock_key := abs(%sh_bigint(p_actor_type || '::' || p_actor_id));
        IF NOT pg_try_advisory_xact_lock(v_actor_lock_key) THEN
            RETURN;
        END IF;
    END IF;

    -- Keep the stored alarm when no allowed host can own its actor while allowing all other placement errors to roll back the operation
    BEGIN
        PERFORM host_id
        FROM %slookup_allocate_actor_v1(
            p_actor_type,
            p_actor_id,
            p_host_health_check_deadline,
            p_host_ids
        );
    EXCEPTION
        WHEN SQLSTATE 'P0001' THEN
            IF SQLERRM = 'NO_HOST_AVAILABLE' THEN
                RETURN;
            END IF;
            RAISE;
    END;

    -- Keep a newly-placed actor active until the alarm is due so the placement cannot idle out during fetch-ahead
    IF NOT v_actor_was_active THEN
        UPDATE %sactive_actors
        SET actor_activation = GREATEST(v_now, v_alarm_due_time)
        WHERE actor_type = p_actor_type AND actor_id = p_actor_id;
    END IF;

    -- Acquire the lease only if the locked row is still eligible
    RETURN QUERY
    UPDATE %salarms
    SET
        alarm_lease_id = gen_random_uuid(),
        alarm_lease_expiration_time = v_now + p_alarms_lease_duration
    WHERE
        alarm_id = v_alarm_id
        AND (
            alarm_lease_id IS NULL
            OR alarm_lease_expiration_time IS NULL
            OR alarm_lease_expiration_time < v_now
        )
    RETURNING
        alarm_id AS r_alarm_id,
        alarm_due_time AS r_alarm_due_time,
        alarm_lease_id AS r_lease_id;
END;
$$ LANGUAGE plpgsql;
