-- Atomically stores one idempotent job and leases it when its actor can run on an allowed host
CREATE OR REPLACE FUNCTION %sdispatch_and_lease_job_v1(
    p_job_id uuid,
    p_actor_type text,
    p_actor_id text,
    p_job_name text,
    p_job_due_time timestamp,
    p_job_interval text,
    p_job_cron text,
    p_job_ttl_time timestamp,
    p_job_data bytea,
    p_job_method text,
    p_host_ids uuid[],
    p_host_health_check_deadline interval,
    p_alarms_fetch_ahead_interval interval,
    p_alarms_lease_duration interval
)
RETURNS TABLE (
    r_job_id uuid,
    r_job_due_time timestamp,
    r_lease_id uuid
) AS $$
DECLARE
    v_now timestamp;
    v_job_id uuid;
    v_job_due_time timestamp;
    v_locked_job_id uuid;
    v_actor_was_active boolean;
    v_actor_lock_key bigint;
BEGIN
    -- Keep the first job stored for an idempotency key and identify whether this call inserted it
    INSERT INTO %salarms
        (alarm_id, actor_type, actor_id, alarm_name,
        alarm_due_time, alarm_interval, alarm_cron, alarm_ttl_time, alarm_data,
        alarm_kind, job_method,
        alarm_lease_id, alarm_lease_expiration_time)
    VALUES
        (p_job_id, p_actor_type, p_actor_id, p_job_name,
        p_job_due_time, p_job_interval, p_job_cron, p_job_ttl_time, p_job_data,
        'job', p_job_method,
        NULL, NULL)
    ON CONFLICT (actor_type, actor_id, alarm_name) DO NOTHING
    RETURNING alarm_id, alarm_due_time INTO v_job_id, v_job_due_time;

    -- An idempotency conflict preserves the original row while allowing an unleased occurrence to become immediately schedulable
    IF NOT FOUND THEN
        SELECT alarm_id, alarm_due_time
        INTO v_job_id, v_job_due_time
        FROM %salarms
        WHERE actor_type = p_actor_type AND actor_id = p_actor_id AND alarm_name = p_job_name;
    END IF;

    -- Keep the new job unleased when the database clock places it outside fetch-ahead
    v_now := now() AT TIME ZONE 'utc';
    IF v_job_due_time > v_now + p_alarms_fetch_ahead_interval THEN
        RETURN QUERY SELECT v_job_id, v_job_due_time, NULL::uuid;
        RETURN;
    END IF;

    -- Lock the job only while its lease is absent or expired
    SELECT alarm_id
    INTO v_locked_job_id
    FROM %salarms
    WHERE
        alarm_id = v_job_id
        AND (
            alarm_lease_id IS NULL
            OR alarm_lease_expiration_time IS NULL
            OR alarm_lease_expiration_time < v_now
        )
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT v_job_id, v_job_due_time, NULL::uuid;
        RETURN;
    END IF;

    -- Avoid waiting for an actor lock while holding the job row because the batch fetcher acquires those locks in the opposite order
    v_actor_was_active := %sactor_active_v1(
        p_actor_type,
        p_actor_id,
        v_now - p_host_health_check_deadline
    );
    IF NOT v_actor_was_active THEN
        v_actor_lock_key := abs(%sh_bigint(p_actor_type || '::' || p_actor_id));
        IF NOT pg_try_advisory_xact_lock(v_actor_lock_key) THEN
            RETURN QUERY SELECT v_job_id, v_job_due_time, NULL::uuid;
            RETURN;
        END IF;
    END IF;

    -- Keep the stored job when no allowed host can own its actor while allowing all other placement errors to roll back the operation
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
                RETURN QUERY SELECT v_job_id, v_job_due_time, NULL::uuid;
                RETURN;
            END IF;
            RAISE;
    END;

    -- Keep a newly-placed actor active until the job is due so the placement cannot idle out during fetch-ahead
    IF NOT v_actor_was_active THEN
        UPDATE %sactive_actors
        SET actor_activation = GREATEST(v_now, v_job_due_time)
        WHERE actor_type = p_actor_type AND actor_id = p_actor_id;
    END IF;

    -- Acquire the lease before returning the job to the runtime
    RETURN QUERY
    UPDATE %salarms
    SET
        alarm_lease_id = gen_random_uuid(),
        alarm_lease_expiration_time = v_now + p_alarms_lease_duration
    WHERE
        alarm_id = v_locked_job_id
        AND (
            alarm_lease_id IS NULL
            OR alarm_lease_expiration_time IS NULL
            OR alarm_lease_expiration_time < v_now
        )
    RETURNING
        alarm_id AS r_job_id,
        alarm_due_time AS r_job_due_time,
        alarm_lease_id AS r_lease_id;

    -- Preserve the durable dispatch result if the lease could not be acquired
    IF NOT FOUND THEN
        RETURN QUERY SELECT v_job_id, v_job_due_time, NULL::uuid;
    END IF;
END;
$$ LANGUAGE plpgsql;
