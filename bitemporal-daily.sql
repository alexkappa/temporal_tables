--
-- Bitemporal tables.
--
-- These tables allow versioning both at the system and application-time period
-- levels (period_modified and period_valid respectively).
--
CREATE OR REPLACE FUNCTION xxx_update_history() RETURNS TRIGGER AS $$
DECLARE
    tx txid_snapshot;
    ts timestamptz := current_timestamp;
    period_valid_lower timestamptz;
    period_valid_upper timestamptz;
    period_modified_lower timestamptz;
    period_modified_upper timestamptz;
BEGIN

    -- Ignore rows already modified in this transaction

    tx := txid_current_snapshot();
    IF OLD.xmin::text >= (txid_snapshot_xmin(tx) % (2^32)::bigint)::text
        AND OLD.xmin::text <= (txid_snapshot_xmax(tx) % (2^32)::bigint)::text THEN
        RETURN NEW;
    END IF;

    -- Mitigate period_modified conflicts in case there is overlap by setting
    -- the upper bound of period_modified equal to the lower bound of
    -- period_modified plus 1 microsecond.

    period_modified_lower := lower(OLD.period_modified);
    IF period_modified_lower >= ts THEN
        ts := period_modified_lower + interval '1 microseconds';
    END IF;
    period_modified_upper := ts;

    IF lower(NEW.period_valid) < lower(OLD.period_valid) THEN

        -- The updated lower bound of period_valid is earlier than the existing
        -- lower bound period_valid. This means that the record we are updating
        -- is a historical one, therefore we only add it to xxx_history but not
        -- xxx (return OLD).
        -- We calculate the historical upper bound of period_valid as one day
        -- after the existing lower bound.

        period_valid_lower := lower(NEW.period_valid);
        period_valid_upper := lower(NEW.period_valid) + '1 day';

        INSERT INTO xxx_history VALUES(
            NEW.id,
            NEW.xxx,
            tstzrange(period_valid_lower, period_valid_upper, '[)'),
            tstzrange(period_modified_lower, period_modified_upper, '[)')
        );

        -- Even though the record is not updated with NEW, we still update the
        -- period_modified in order to allow continuation in system temporal
        -- changes.

        OLD.period_modified := tstzrange(ts, null);

        RETURN OLD;
    ELSE

        -- The updated lower bound of period_valid is later than the existing lower bound
        -- period_valid. This means the record is allowed to update xxx as well as
        -- xxx_history.

        period_valid_lower := lower(OLD.period_valid);
        period_valid_upper := lower(OLD.period_valid) + '1 day';

        INSERT INTO xxx_history VALUES(
            OLD.id,
            OLD.xxx,
            tstzrange(period_valid_lower, period_valid_upper, '[)'),
            tstzrange(period_modified_lower, period_modified_upper, '[)')
        );

        NEW.period_modified := tstzrange(ts, null);

        RETURN NEW;
    END IF;

END;
$$ LANGUAGE plpgsql;

CREATE TABLE xxx (
     id int,
     xxx text,
     period_valid tstzrange NOT NULL,
     period_modified tstzrange NOT NULL DEFAULT tstzrange(current_timestamp, null)
);

CREATE TABLE xxx_history (LIKE xxx);

DROP TRIGGER IF EXISTS xxx_update_trigger ON xxx;
CREATE TRIGGER xxx_update_trigger
    BEFORE UPDATE ON xxx
    FOR EACH ROW
EXECUTE PROCEDURE xxx_update_history();

TRUNCATE xxx;
TRUNCATE xxx_history;
INSERT INTO xxx VALUES (1, 'aaa', '[2016-01-01,)');
UPDATE xxx SET xxx = 'bbb', period_valid = '[2016-01-02,)' WHERE id = 1;
UPDATE xxx SET xxx = 'ccc', period_valid = '[2016-01-03,)' WHERE id = 1;
UPDATE xxx SET xxx = 'zzz', period_valid = '[2015-01-01,)' WHERE id = 1; -- no effect on xxx
UPDATE xxx SET xxx = 'ddd', period_valid = '[2016-01-04,)' WHERE id = 1;
UPDATE xxx SET xxx = 'fff', period_valid = '[2016-01-06,)' WHERE id = 1;
UPDATE xxx SET xxx = 'eee', period_valid = '[2016-01-05,)' WHERE id = 1;
UPDATE xxx SET xxx = 'iii', period_valid = '[2016-01-09,)' WHERE id = 1;
UPDATE xxx SET xxx = 'ggg', period_valid = '[2016-01-07,)' WHERE id = 1; -- no effect on xxx
UPDATE xxx SET xxx = 'iii-1' WHERE id = 1; -- no effect on xxx_history
UPDATE xxx SET xxx = 'iii-2' WHERE id = 1; -- no effect on xxx_history
UPDATE xxx SET xxx = 'iii-3' WHERE id = 1; -- no effect on xxx_history