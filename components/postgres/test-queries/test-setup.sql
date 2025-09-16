-- Performs the required setup for testing of the Postgres component
-- Sources:
-- https://gist.github.com/thehesiod/d0314d599c721216f075375c667e2d9a
-- https://dba.stackexchange.com/questions/69988/how-can-i-fake-inet-client-addr-for-unit-tests-in-postgresql/70009#70009

CREATE TABLE IF NOT EXISTS freeze_time_key_type (
    key_type text NOT NULL PRIMARY KEY
);
INSERT INTO freeze_time_key_type
    VALUES
        ('enabled'), ('timestamp'), ('tick')
    ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS freeze_time_config (
    key text NOT NULL PRIMARY KEY REFERENCES freeze_time_key_type(key_type),
    value jsonb
);

INSERT INTO freeze_time_config
    VALUES
        ('enabled', 'false'),
        ('timestamp', 'null'),
        ('tick', 'false')
    ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION freeze_time(
    freeze_time timestamptz,
    tick bool DEFAULT FALSE
)
RETURNS void AS
$$
BEGIN
    INSERT INTO freeze_time_config(key, value) VALUES
        ('enabled', 'true'),
        ('timestamp', EXTRACT(EPOCH FROM freeze_time)::text::jsonb),
        ('tick', tick::text::jsonb)
    ON CONFLICT(key) DO UPDATE SET
        value = EXCLUDED.value;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION unfreeze_time()
RETURNS void AS
$$
BEGIN
    INSERT INTO freeze_time_config(key, value) VALUES
        ('enabled', 'false')
    ON CONFLICT(key) do update set
        value = excluded.value;
END
$$ LANGUAGE plpgsql;

-- NOTE: we cannot create functions that take no parameters like CURRENT_DATE so your SQL should always use now()
-- NOTE: the search_path order is: https://www.postgresonline.com/article_pfriendly/279.html
CREATE OR REPLACE FUNCTION now()
RETURNS timestamptz AS
$$
DECLARE enabled text;
DECLARE tick text;
DECLARE timestamp timestamp;
BEGIN
    SELECT INTO enabled VALUE FROM freeze_time_config WHERE key = 'enabled';
    SELECT INTO tick VALUE FROM freeze_time_config WHERE key = 'tick';

    IF enabled THEN
        SELECT INTO timestamp to_timestamp(value::text::decimal) FROM freeze_time_config WHERE key = 'timestamp';

        IF tick THEN
            timestamp = timestamp + '1 second'::interval;
            UPDATE freeze_time_config SET VALUE = extract(epoch FROM timestamp)::text::jsonb WHERE key = 'timestamp';
        END IF;

        RETURN timestamp;
    ELSE
        RETURN pg_catalog.now();

    END IF;
END
$$ LANGUAGE plpgsql;
