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
    v_allowed_host uuid;
BEGIN
    -- First, check if the actor is already active on any healthy host
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

    -- If we reach here, we need to find a suitable host and activate the actor
    -- Build the query to find an available host
    WITH available_hosts AS (
        SELECT 
            h.host_id,
            h.host_address,
            hat.actor_idle_timeout
        FROM hosts h
        JOIN host_actor_types hat ON h.host_id = hat.host_id
        LEFT JOIN host_active_actor_count haac ON (
            h.host_id = haac.host_id 
            AND hat.actor_type = haac.actor_type
        )
        WHERE hat.actor_type = p_actor_type
          AND h.host_last_health_check >= (now() - p_host_health_check_deadline)
          AND (
                hat.actor_concurrency_limit = 0 
                OR COALESCE(haac.active_count, 0) < hat.actor_concurrency_limit
          )
          AND (
                p_allowed_hosts IS NULL 
                OR array_length(p_allowed_hosts, 1) = 0 
                OR h.host_id = ANY(p_allowed_hosts)
          )
        ORDER BY random()
        LIMIT 1
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
END;
$$ LANGUAGE plpgsql;
