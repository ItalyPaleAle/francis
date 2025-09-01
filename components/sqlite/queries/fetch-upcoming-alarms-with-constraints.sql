-- Fetches the upcoming alarms with constraints on capacity

-- Parameters:
-- 1. Host health check cutoff, as UNIX timestamp with ms (int)
-- 2. Lookahead time horizon, as UNIX timestamp with ms (int)
-- 3. Current timestamp, as UNIX timestamp with ms (int)
-- 4. Capacity cap per actor type (typically batch size) (int)
-- 5. Batch size (int)

-- How the query works:
--
-- 1. allowed_actor_hosts:
--    This CTE is necessary to look up what hosts are ok when we see that the actor mapped to an alarm is active.
--    Some alarms are for actors that are not active, but some may be mapped to actors that are already active.
--    We accept alarms mapping to an active actor if they either:
--      - Map to an actor that's active on a host in the allowlist
--      - Map to an actor that's active on an unhealthy host
--    The CTE loads host IDs both from the pre-filtered allowlist (the temp_capacities table), and from the
--    active_actors table, looking at all the actors of the types we care about (those that can be executed on
--    the hosts in the allowlist).
-- 2. actor_type_capacity:
--    This CTE computes the sum of the available capacity for each actor type, across all hosts in the allowlist.
-- 3. ranked:
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
--    of execution time. We do this by excluding the rows from ranked in which the row number is greater than the
--    capacity left (e.g. if we have capacity for only 4 actors of type A, ranked rows for type A with row number
--    greater than 4 are excluded).
--
-- Alarms that have an active actor will have host_id non-null. However, that will be non-null also for actors that
-- are on un-healthy hosts; we will need to filter them out in the Go code later.

WITH
    allowed_actor_hosts AS (
        SELECT DISTINCT host_id
        FROM temp_capacities

        UNION

        SELECT DISTINCT aa.host_id
        FROM active_actors AS aa
        INNER JOIN temp_capacities AS cap
            USING (actor_type)
        INNER JOIN hosts AS h
            USING (host_id)
        WHERE
            h.host_last_health_check < ?
    ),
    actor_type_capacity AS (
        SELECT actor_type, min(sum(capacity), ?) AS total_capacity
        FROM temp_capacities
        GROUP BY actor_type
    ),
    ranked AS (
        SELECT
            a.alarm_id, a.actor_type, a.actor_id, a.alarm_due_time,
            aa.host_id,
            atc.total_capacity,
            CASE
                WHEN
                    aa.host_id IS NULL
                    OR NOT EXISTS (SELECT 1 FROM temp_capacities WHERE temp_capacities.host_id = aa.host_id)
                THEN 0
                ELSE 1
            END AS active_actor,
            SUM(1)
            FILTER (
                WHERE aa.host_id IS NULL
                OR NOT EXISTS (SELECT 1 FROM temp_capacities WHERE temp_capacities.host_id = aa.host_id)
            )
            OVER (
                PARTITION BY a.actor_type
                ORDER BY a.alarm_due_time, a.alarm_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS rownum
        FROM alarms AS a
        INNER JOIN actor_type_capacity AS atc
            USING (actor_type)
        LEFT JOIN active_actors AS aa
            USING (actor_type, actor_id)
        WHERE 
            a.alarm_due_time <= ?
            AND (
                a.alarm_lease_id IS NULL
                OR a.alarm_lease_expiration_time IS NULL
                OR a.alarm_lease_expiration_time < ?
            )
            AND (
                aa.host_id IS NULL
                OR aa.host_id IN (SELECT host_id FROM allowed_actor_hosts)
            )
    )
SELECT
    alarm_id, actor_type, actor_id, alarm_due_time, host_id
FROM ranked
WHERE
    active_actor = 1
    OR rownum <= total_capacity
ORDER BY alarm_due_time ASC, alarm_id
LIMIT ?
