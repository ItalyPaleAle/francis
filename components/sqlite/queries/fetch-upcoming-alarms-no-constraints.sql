-- Fetches the upcoming alarms without constraints on capacity

-- Parameters:
-- 1. Host health check cutoff, as UNIX timestamp with ms (int)
-- 2. Lookahead time horizon, as UNIX timestamp with ms (int)
-- 3. Current timestamp, as UNIX timestamp with ms (int)
-- 4. Batch size (int)

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
-- 2. Look up alarms:
--    Next, we can look up the list of alarms, looking at the N-most alarms that are coming up the soonest.
--    We filter the alarms by:
--      - Limiting to the actor types that can be executed on the hosts in the request
--        (this is done with the INNER JOIN on temp_capacities)
--      - Ensuring their due time is within the horizon we are considering
--      - Selecting alarms that aren't leased, or whose lease has expired
--      - Selecting alarms that are not tied to an active actor, or whose actor is in the allowed_actor_hosts list
--
-- Alarms that have an active actor will have host_id non-null. However, that will be non-null also for actors that
-- are on un-healthy hosts; we will need to filter them out in the Go code later.

WITH
    allowed_actor_hosts AS (
        SELECT host_id
        FROM temp_capacities

        UNION

        SELECT aa.host_id
        FROM active_actors AS aa
        INNER JOIN temp_capacities AS cap
            USING (actor_type)
        INNER JOIN hosts AS h
            USING (host_id)
        WHERE
            h.host_last_health_check < ?
    )
SELECT a.alarm_id, a.actor_type, a.actor_id, a.alarm_due_time, aa.host_id
FROM alarms AS a
INNER JOIN temp_capacities AS cap
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
        OR EXISTS (
            SELECT 1
            FROM allowed_actor_hosts AS ah
            WHERE ah.host_id = aa.host_id
        )
    )
ORDER BY alarm_due_time ASC, alarm_id
LIMIT ?
