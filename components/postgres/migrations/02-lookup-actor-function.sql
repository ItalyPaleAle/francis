-- Helper function to check if an actor is already active and enforce host restrictions
CREATE OR REPLACE FUNCTION check_active_actor_v1(
    p_actor_type text,
    p_actor_id text,
    p_host_health_check_deadline interval,
    p_allowed_hosts uuid[] DEFAULT NULL
)
RETURNS TABLE (
    host_id uuid,
    host_address text,
    idle_timeout interval
) AS $$
DECLARE
    v_host_id uuid;
    v_host_address text;
    v_idle_timeout interval;
BEGIN
    -- Check if the actor is already active on any healthy host
    SELECT h.host_id, h.host_address, aa.actor_idle_timeout
    INTO v_host_id, v_host_address, v_idle_timeout
    FROM active_actors aa
    JOIN hosts h ON aa.host_id = h.host_id
    WHERE aa.actor_type = p_actor_type
        AND aa.actor_id = p_actor_id
        AND h.host_last_health_check >= (now() - p_host_health_check_deadline);

    -- If we found an active actor
    IF FOUND THEN
        -- Check host restrictions if any are specified
        IF p_allowed_hosts IS NOT NULL AND array_length(p_allowed_hosts, 1) > 0 THEN
            -- Check if the current host is in the allowed list
            IF v_host_id = ANY(p_allowed_hosts) THEN
                -- Host is allowed, return the result
                host_id := v_host_id;
                host_address := v_host_address;
                idle_timeout := v_idle_timeout;
                RETURN NEXT;
                RETURN;
            ELSE
                -- Actor is on a disallowed host, raise an exception
                RAISE EXCEPTION 'NO_HOST_AVAILABLE' USING ERRCODE = 'P0001';
            END IF;
        ELSE
            -- No host restrictions, return the active actor
            host_id := v_host_id;
            host_address := v_host_address;
            idle_timeout := v_idle_timeout;
            RETURN NEXT;
            RETURN;
        END IF;
    END IF;

    -- If we reach here, no active actor was found
    RETURN;
END;
$$ LANGUAGE plpgsql;

-- Performs a lookup for an actor
-- If the actor is active, returns it, unless it violates the allowed host constraints
-- Otherwise, activates the actor on one of the allowed hosts
CREATE OR REPLACE FUNCTION lookup_actor_v1(
    p_actor_type text,
    p_actor_id text,
    p_host_health_check_deadline interval,
    p_allowed_hosts uuid[] DEFAULT NULL
)
RETURNS TABLE (
    host_id uuid,
    host_address text,
    idle_timeout interval
) AS $$
DECLARE
    v_host_id uuid;
    v_host_address text;
    v_idle_timeout interval;
    v_lock_key bigint;
    v_result RECORD;
BEGIN
    -- First, check if the actor is already active on any healthy host
    SELECT * INTO v_result 
    FROM check_active_actor_v1(p_actor_type, p_actor_id, p_host_health_check_deadline, p_allowed_hosts);

    IF FOUND THEN
        host_id := v_result.host_id;
        host_address := v_result.host_address;
        idle_timeout := v_result.idle_timeout;
        RETURN NEXT;
        RETURN;
    END IF;

    -- Create a deterministic lock key based on actor type and ID
    -- This ensures the same actor always gets the same lock
    v_lock_key := abs(h_bigint(p_actor_type || '::' || p_actor_id));

    -- Acquire an advisory lock for this specific actor
    -- This prevents concurrent placement of the same actor
    PERFORM pg_advisory_xact_lock(v_lock_key);

    -- Check again if the actor is already active, as it may have gotten activated since we got the lock
    SELECT * INTO v_result 
    FROM check_active_actor_v1(p_actor_type, p_actor_id, p_host_health_check_deadline, p_allowed_hosts);

    IF FOUND THEN
        host_id := v_result.host_id;
        host_address := v_result.host_address;
        idle_timeout := v_result.idle_timeout;
        RETURN NEXT;
        RETURN;
    END IF;

    -- If we reach here, we need to find a suitable host and activate the actor
    -- Use a SELECT FOR UPDATE to lock the host records while we're making our decision
    WITH
        current_count AS (
            -- Count actual rows instead of using the view to avoid race conditions
            SELECT aa.host_id, COUNT(*) AS active_count
            FROM active_actors aa
            WHERE aa.actor_type = p_actor_type
            GROUP BY aa.host_id
        ),
        available_hosts AS (
            SELECT 
                h.host_id,
                h.host_address,
                hat.actor_idle_timeout,
                hat.actor_concurrency_limit,
                COALESCE(current_count.active_count, 0) AS current_active_count
            FROM hosts h
            JOIN host_actor_types hat ON h.host_id = hat.host_id
            LEFT JOIN current_count ON h.host_id = current_count.host_id
            WHERE
                hat.actor_type = p_actor_type
            AND h.host_last_health_check >= (now() - p_host_health_check_deadline)
            AND (
                    hat.actor_concurrency_limit = 0 
                    OR COALESCE(current_count.active_count, 0) < hat.actor_concurrency_limit
            )
            AND (
                    p_allowed_hosts IS NULL 
                    OR array_length(p_allowed_hosts, 1) = 0 
                    OR h.host_id = ANY(p_allowed_hosts)
            )
            ORDER BY 
                -- Prefer hosts with lower current load for better distribution
                current_active_count ASC,
                -- Then randomize among hosts with same load
                random()
            LIMIT 1
            -- Lock the host rows to prevent concurrent modifications
            FOR UPDATE OF h
        )
    SELECT ah.host_id, ah.host_address, ah.actor_idle_timeout
    INTO v_host_id, v_host_address, v_idle_timeout
    FROM available_hosts ah;

    -- If no suitable host was found
    IF NOT FOUND THEN
        RAISE EXCEPTION 'NO_HOST_AVAILABLE' USING ERRCODE = 'P0001';
    END IF;

    -- Finally, insert the row in the active actors table to "activate" the actor, in the host we selected
	-- Note that we perform an upsert query here. This is because the actor (with same type and ID) may already be present in the table, where it's active on a host that has failed (but hasn't been garbage-collected yet)
    INSERT INTO active_actors (
        actor_type,
        actor_id,
        host_id,
        actor_idle_timeout,
        actor_activation
    ) VALUES (
        p_actor_type,
        p_actor_id,
        v_host_id,
        v_idle_timeout,
        now()
    )
    ON CONFLICT (actor_type, actor_id) DO UPDATE SET
        host_id = EXCLUDED.host_id,
        actor_idle_timeout = EXCLUDED.actor_idle_timeout,
        actor_activation = EXCLUDED.actor_activation;

    -- Return the selected host information
    host_id := v_host_id;
    host_address := v_host_address;
    idle_timeout := v_idle_timeout;
    RETURN NEXT;

    -- Advisory lock is automatically released at transaction end
END;
$$ LANGUAGE plpgsql;
