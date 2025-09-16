-- Create a PL/pgSQL function to fetch and lease upcoming alarms
CREATE OR REPLACE FUNCTION fetch_and_lease_upcoming_alarms_v1(
    p_host_ids uuid[],
    p_host_health_check_deadline interval,
    p_alarms_fetch_ahead_interval interval,
    p_alarms_lease_duration interval,
    p_batch_size integer
)
RETURNS TABLE (
    r_alarm_id uuid,
    r_actor_type text,
    r_actor_id text,
    r_alarm_name text,
    r_alarm_due_time timestamptz,
    r_lease_id uuid
) AS $$
DECLARE
    v_now timestamptz;
    v_horizon timestamptz;
    v_health_cutoff timestamptz;
    v_lease_expiration timestamptz;
    v_has_capacity_limits boolean := false;
    v_allocated_host_id uuid;
    v_actor_lock_key bigint;
    rec RECORD;
BEGIN
    -- Initialize time variables
    v_now := now();
    v_horizon := v_now + p_alarms_fetch_ahead_interval;
    v_health_cutoff := v_now - p_host_health_check_deadline;
    v_lease_expiration := v_now + p_alarms_lease_duration;

    -- Create temporary table for active hosts with their capacities
    CREATE TEMPORARY TABLE temp_active_hosts (
        host_id uuid NOT NULL,
        actor_type text NOT NULL,
        actor_idle_timeout interval NOT NULL,
        concurrency_limit integer NOT NULL,
        available_capacity integer NOT NULL,
        PRIMARY KEY (host_id, actor_type)
    ) ON COMMIT DROP;

    -- Populate the temporary table with active hosts and their available capacities
    WITH
        current_count AS (
            SELECT aa.host_id, aa.actor_type, COUNT(*) AS active_count
            FROM active_actors AS aa
            GROUP BY aa.host_id, aa.actor_type
        )
    INSERT INTO temp_active_hosts
        (host_id, actor_type, actor_idle_timeout, concurrency_limit, available_capacity)
    SELECT 
        hat.host_id,
        hat.actor_type,
        hat.actor_idle_timeout,
        COALESCE(hat.actor_concurrency_limit, 0) AS concurrency_limit,
        CASE 
            -- If there's no limit, assume it's MaxInt32
            WHEN hat.actor_concurrency_limit = 0 THEN 2147483647 - COALESCE(current_count.active_count, 0)
            ELSE GREATEST(0, hat.actor_concurrency_limit - COALESCE(current_count.active_count, 0))
        END AS available_capacity
    FROM host_actor_types AS hat
    JOIN hosts h ON
        hat.host_id = h.host_id
    LEFT JOIN current_count ON
        hat.host_id = current_count.host_id
        AND hat.actor_type = current_count.actor_type
    WHERE
        h.host_last_health_check >= v_health_cutoff
        AND h.host_id = ANY(p_host_ids);

    -- Early return if no active hosts found
    IF NOT EXISTS (SELECT 1 FROM temp_active_hosts) THEN
        RETURN;
    END IF;

    -- Create temporary table for upcoming alarms
    CREATE TEMPORARY TABLE temp_upcoming_alarms (
        alarm_id uuid NOT NULL,
        actor_type text NOT NULL,
        actor_id text NOT NULL,
        alarm_name text NOT NULL,
        alarm_due_time timestamptz NOT NULL,
        existing_host_id uuid,
        allocated_host_id uuid
    ) ON COMMIT DROP;

    -- Check if we have any capacity limits
    SELECT EXISTS (
        SELECT 1 FROM temp_active_hosts
        WHERE concurrency_limit > 0
    )
    INTO v_has_capacity_limits;

    -- Fetch upcoming alarms based on whether we have capacity constraints
    IF v_has_capacity_limits THEN
        -- How the query works:
        --
        -- 1. allowed_actor_hosts:
        --    This CTE is necessary to look up what hosts are ok when we see that the actor mapped to an alarm is active.
        --    Some alarms are for actors that are not active, but some may be mapped to actors that are already active.
        --    We accept alarms mapping to an active actor if they either:
        --      - Map to an actor that's active on a host in the allowlist
        --      - Map to an actor that's active on an unhealthy host
        --    The CTE loads host IDs both from the pre-filtered allowlist (the temp_active_hosts table), and from the
        --    active_actors table, looking at all the actors of the types we care about (those that can be executed on
        --    the hosts in the allowlist).
        -- 2. actor_type_capacity:
        --    This CTE computes the sum of the available capacity for each actor type, across all hosts in the allowlist.
        -- 3. ranked_alarms:
        --    This CTE looks up the list of alarms and assigns a "rank".
        --    Alarms are filtered by:
        --      - Limiting to the actor types that can be executed on the hosts in the request and for which we have any
        --        capacity (this is done with the INNER JOIN on actor_type_capacity)
        --      - Ensuring their due time is within the horizon we are considering
        --      - Selecting alarms that aren't leased, or whose lease has expired
        --      - Selecting alarms that are not tied to an active actor, or whose actor is in the allowed_actor_hosts list
        --    Among all the filtered alarms, it assigns a "rank" which is the ROW_NUMBER(). This is sorted by the due time
        --    and partitioned by actor type. It looks at all filtered alarms (more than the batch size, but still within
        --    the time horizon and with the other filters listed above), and assigns a rank for each actor type: for example,
        --    if we have capacity for alarms of types A and B, the earliest alarm of type A will have rank 1, and so will
        --    the earliest of type B.
        --    There's one exception, which is that when the alarm is for an actor that's already active on a host in the
        --    allowlist, we assign it a rank of 0, as it doesn't use more capacity.
        --    This "ranking" selects a lot or rows, and it's the reason why this is the "slow" path.
        -- 4. Finally, select from the ranked list, returning alarms for which there's sufficient capacity, in order of
        --    of execution time. We do this by excluding the rows from ranked_alarms in which the row number is greater than the
        --    capacity left (e.g. if we have capacity for only 4 actors of type A, ranked rows for type A with row number
        --    greater than 4 are excluded).
        --
        -- Alarms that have an active actor will have host_id non-null. However, that will be non-null also for actors that
        -- are on un-healthy hosts; we will need to filter them out in the Go code later.
        INSERT INTO temp_upcoming_alarms
            (alarm_id, actor_type, actor_id, alarm_name, alarm_due_time, existing_host_id)
        WITH
            allowed_actor_hosts AS (
                SELECT tah.host_id FROM temp_active_hosts AS tah

                UNION

                SELECT aa.host_id
                FROM active_actors AS aa
                JOIN temp_active_hosts AS tah ON
                    aa.actor_type = tah.actor_type
                JOIN hosts AS h ON
                    aa.host_id = h.host_id
                WHERE
                    h.host_last_health_check < v_health_cutoff
            ),
            actor_type_capacity AS (
                SELECT tah.actor_type, LEAST(SUM(tah.available_capacity), p_batch_size) AS total_capacity
                FROM temp_active_hosts AS tah
                GROUP BY tah.actor_type
            ),
            ranked_alarms AS (
                SELECT 
                    a.alarm_id, a.actor_type, a.actor_id,
                    a.alarm_name, a.alarm_due_time,
                    aa.host_id AS existing_host_id,
                    atc.total_capacity,
                    CASE
                WHEN
                    aa.host_id IS NULL
                    OR NOT EXISTS (SELECT 1 FROM temp_active_hosts WHERE temp_active_hosts.host_id = aa.host_id)
                        THEN 0
                        ELSE 1
                    END AS active_actor,
                    SUM(1)
                    FILTER (
                        WHERE aa.host_id IS NULL
                        OR NOT EXISTS (SELECT 1 FROM temp_active_hosts WHERE temp_active_hosts.host_id = aa.host_id)
                    )
                    OVER (
                        PARTITION BY a.actor_type
                        ORDER BY a.alarm_due_time, a.alarm_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS rownum
                FROM alarms AS a
                JOIN actor_type_capacity AS atc ON
                    a.actor_type = atc.actor_type
                LEFT JOIN active_actors AS aa ON
                    a.actor_type = aa.actor_type
                    AND a.actor_id = aa.actor_id
                WHERE
                    a.alarm_due_time <= v_horizon
                    AND (
                        a.alarm_lease_id IS NULL
                        OR a.alarm_lease_expiration_time IS NULL
                        OR a.alarm_lease_expiration_time < v_now
                    )
                    AND (
                        aa.host_id IS NULL
                        OR EXISTS (
                            SELECT 1
                            FROM allowed_actor_hosts AS ah
                            WHERE ah.host_id = aa.host_id
                        )
                    )
            )
        SELECT 
            alarm_id,
            actor_type,
            actor_id,
            alarm_name,
            alarm_due_time,
            existing_host_id
        FROM ranked_alarms
        WHERE
            active_actor = 1
            OR rownum <= total_capacity
        ORDER BY alarm_due_time, alarm_id
        LIMIT p_batch_size;
    ELSE
        -- How the query works:
        --
        -- 1. allowed_actor_hosts:
        --    This CTE is necessary to look up what hosts are ok when we see that the actor mapped to an alarm is active.
        --    Some alarms are for actors that are not active, but some may be mapped to actors that are already active.
        --    We accept alarms mapping to an active actor if they either:
        --      - Map to an actor that's active on a host in the allowlist
        --      - Map to an actor that's active on an unhealthy host
        --    The CTE loads host IDs both from the pre-filtered allowlist (the temp_active_hosts table), and from the
        --    active_actors table, looking at all the actors of the types we care about (those that can be executed on
        --    the hosts in the allowlist).
        -- 2. Look up alarms:
        --    Next, we can look up the list of alarms, looking at the N-most alarms that are coming up the soonest.
        --    We filter the alarms by:
        --      - Limiting to the actor types that can be executed on the hosts in the request
        --        (this is done with the INNER JOIN on temp_active_hosts)
        --      - Ensuring their due time is within the horizon we are considering
        --      - Selecting alarms that aren't leased, or whose lease has expired
        --      - Selecting alarms that are not tied to an active actor, or whose actor is in the allowed_actor_hosts list
        --
        -- Alarms that have an active actor will have host_id non-null. However, that will be non-null also for actors that
        -- are on un-healthy hosts; we will need to filter them out later.
        INSERT INTO temp_upcoming_alarms
            (alarm_id, actor_type, actor_id, alarm_name, alarm_due_time, existing_host_id)
        WITH
            allowed_actor_hosts AS (
                SELECT tah.host_id FROM temp_active_hosts AS tah
                UNION
                SELECT aa.host_id
                FROM active_actors AS aa
                JOIN temp_active_hosts AS tah ON
                    aa.actor_type = tah.actor_type
                JOIN hosts AS h ON
                    aa.host_id = h.host_id
                WHERE
                    h.host_last_health_check < v_health_cutoff
        )
        SELECT 
            a.alarm_id, a.actor_type, a.actor_id,
            a.alarm_name, a.alarm_due_time,
            aa.host_id AS existing_host_id
        FROM alarms AS a
        INNER JOIN temp_active_hosts AS tah ON
            a.actor_type = tah.actor_type
        LEFT JOIN active_actors AS aa ON
            a.actor_type = aa.actor_type
            AND a.actor_id = aa.actor_id
        WHERE
            a.alarm_due_time <= v_horizon
            AND (
                a.alarm_lease_id IS NULL
                OR a.alarm_lease_expiration_time IS NULL
                OR a.alarm_lease_expiration_time < v_now
            )
            AND (
                aa.host_id IS NULL
                OR EXISTS (
                    SELECT 1
                    FROM allowed_actor_hosts AS ah
                    WHERE ah.host_id = aa.host_id
                )
            )
        ORDER BY a.alarm_due_time, a.alarm_id
        LIMIT p_batch_size;
    END IF;

    -- Filter out alarms with actors on unhealthy hosts
    UPDATE temp_upcoming_alarms 
    SET existing_host_id = NULL 
    WHERE
        existing_host_id IS NOT NULL 
        AND NOT EXISTS (
            SELECT 1 FROM temp_active_hosts AS tah
            WHERE
                tah.host_id = temp_upcoming_alarms.existing_host_id
                AND tah.actor_type = temp_upcoming_alarms.actor_type
        );

    -- Early return if no alarms found
    IF NOT EXISTS(SELECT 1 FROM temp_upcoming_alarms) THEN
        RETURN;
    END IF;

    -- Allocate actors for alarms that don't have existing actors
    FOR rec IN 
        SELECT tua.alarm_id, tua.actor_type, tua.actor_id, tua.alarm_due_time
        FROM temp_upcoming_alarms AS tua 
        WHERE tua.existing_host_id IS NULL
    LOOP
        -- Create deterministic lock key from actor type and ID to prevent double activation
        v_actor_lock_key := abs(hashtext(rec.actor_type || '::' || rec.actor_id));

        -- Try to acquire advisory lock for this specific actor
        IF pg_try_advisory_lock(v_actor_lock_key) THEN
            BEGIN
                -- Check if actor already exists (another process might have created it)
                IF NOT EXISTS (
                    SELECT 1 FROM active_actors 
                    WHERE actor_type = rec.actor_type 
                    AND actor_id = rec.actor_id
                )
                THEN
                    -- Find a random host with capacity for this actor type
                    -- Note: There's a chance that multiple queries may allocate actors on the same hosts and we may go over capacity
                    -- We consider this an acceptable risk, as the complexity of handling that case is too significant otherwise
                    SELECT tah.host_id INTO v_allocated_host_id
                    FROM temp_active_hosts AS tah 
                    WHERE
                        tah.actor_type = rec.actor_type 
                        AND tah.available_capacity > 0
                    ORDER BY 
                        -- Prefer hosts with lower current load for better distribution
                        available_capacity DESC,
                        -- Then randomize among hosts with same load
                        random()
                    LIMIT 1;

                    IF v_allocated_host_id IS NOT NULL THEN
                        -- Insert/update the actor
                        -- Note that we perform an upsert query here. This is because the actor (with same type and ID) may already be present in the table, where it's active on a host that has failed (but hasn't been garbage-collected yet)
                        INSERT INTO active_actors
                            (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
                        SELECT 
                            rec.actor_type, 
                            rec.actor_id, 
                            v_allocated_host_id, 
                            tah.actor_idle_timeout,
                            -- We set the alarm's due time as actor activation time, or the current time if that's later
                            GREATEST(rec.alarm_due_time, v_now)
                        FROM temp_active_hosts AS tah 
                        WHERE
                            tah.host_id = v_allocated_host_id
                            AND tah.actor_type = rec.actor_type
                        ON CONFLICT (actor_type, actor_id) DO UPDATE SET
                            host_id = EXCLUDED.host_id,
                            actor_idle_timeout = EXCLUDED.actor_idle_timeout,
                            actor_activation = EXCLUDED.actor_activation;

                        -- Update the allocated host in our temp table
                        UPDATE temp_upcoming_alarms 
                        SET allocated_host_id = v_allocated_host_id 
                        WHERE alarm_id = rec.alarm_id;

                        -- Decrease available capacity
                        UPDATE temp_active_hosts 
                        SET
                            available_capacity = available_capacity - 1 
                        WHERE 
                            host_id = v_allocated_host_id
                            AND actor_type = rec.actor_type;

                        -- Clear the variable for next iteration
                        v_allocated_host_id := NULL;
                    END IF; -- End of v_allocated_host_id not null
                END IF;  -- End of actor existence check
            EXCEPTION
                WHEN OTHERS THEN
                    -- Release lock on any error and re-raise
                    PERFORM pg_advisory_unlock(v_actor_lock_key);
                    RAISE;
            END;

            -- Release the advisory lock
            PERFORM pg_advisory_unlock(v_actor_lock_key);
        END IF;
    END LOOP;

    -- Finally, acquire leases on all alarms that have actors (existing or allocated)
    RETURN QUERY
    UPDATE alarms 
    SET 
        alarm_lease_id = gen_random_uuid(),
        alarm_lease_expiration_time = v_lease_expiration
    FROM temp_upcoming_alarms AS tua
    WHERE
        alarms.alarm_id = tua.alarm_id
        AND (
            tua.existing_host_id IS NOT NULL
            OR tua.allocated_host_id IS NOT NULL
        )
        AND (
            alarms.alarm_lease_id IS NULL
            OR alarms.alarm_lease_expiration_time IS NULL
            OR alarms.alarm_lease_expiration_time < v_now
        )
    RETURNING 
        alarms.alarm_id AS r_alarm_id,
        alarms.actor_type AS r_actor_type,
        alarms.actor_id AS r_actor_id,
        alarms.alarm_name AS r_alarm_name,
        alarms.alarm_due_time AS r_alarm_due_time,
        alarms.alarm_lease_id AS r_lease_id;

END;
$$ LANGUAGE plpgsql;
