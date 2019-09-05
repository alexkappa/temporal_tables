CREATE OR REPLACE FUNCTION xxx_update_history() RETURNS TRIGGER AS $$
DECLARE
    tx txid_snapshot;
    period_valid_lower timestamptz;
    period_valid_upper timestamptz;
BEGIN

    tx := txid_current_snapshot();
    IF OLD.xmin::text >= (txid_snapshot_xmin(tx) % (2^32)::bigint)::text
        AND OLD.xmin::text <= (txid_snapshot_xmax(tx) % (2^32)::bigint)::text THEN
        RETURN NEW;
    END IF;

    IF lower(NEW.period_valid) < lower(OLD.period_valid) THEN

        -- The updated lower bound of period_valid is earlier than the existing lower bound
        -- period_valid. This means that the record we are updating is a historical one,
        -- therefore we only add it to xxx_history but not xxx (return null).
        --
        -- We calculate the historical upper bound of period_valid as the closest known lower
        -- bound period_valid.

        period_valid_lower := lower(NEW.period_valid);

        SELECT MIN(xxx_combined.min_lower_period_valid)
        FROM (
             SELECT MIN(lower(period_valid)) AS min_lower_period_valid
             FROM xxx
             WHERE id = 1 AND lower(period_valid) > period_valid_lower
             UNION ALL
             SELECT MIN(lower(period_valid)) AS min_lower_period_valid
             FROM xxx_history
             WHERE id = 1 AND lower(period_valid) > period_valid_lower
         ) AS xxx_combined INTO period_valid_upper;

        UPDATE xxx_history
        SET period_valid = tstzrange(lower(period_valid), period_valid_lower, '[)')
        WHERE id = NEW.id AND period_valid @> period_valid_lower;

        INSERT INTO xxx_history
        VALUES (
            NEW.id,
            NEW.xxx,
            tstzrange(period_valid_lower, period_valid_upper, '[)')
        );

        RETURN NULL;
    ELSE

        -- The updated lower bound of period_valid is later than the existing lower bound
        -- period_valid. This means the record is allowed to update xxx as well as
        -- xxx_history.

        period_valid_lower := lower(OLD.period_valid);
        period_valid_upper := lower(NEW.period_valid);

        INSERT INTO xxx_history
        VALUES (
            OLD.id,
            OLD.xxx,
            tstzrange(period_valid_lower, period_valid_upper, '[)')
        );

        RETURN NEW;
    END IF;

END;
$$ LANGUAGE plpgsql;

CREATE TABLE xxx (
    id int,
    xxx text,
    period_valid tstzrange NOT NULL DEFAULT tstzrange(current_timestamp, null)
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
UPDATE xxx SET xxx = 'yyy', period_valid = '[2015-01-02,)' WHERE id = 1; -- no effect on xxx
UPDATE xxx SET xxx = 'ddd', period_valid = '[2016-01-04,)' WHERE id = 1;
UPDATE xxx SET xxx = 'fff', period_valid = '[2016-01-06,)' WHERE id = 1;
UPDATE xxx SET xxx = 'eee', period_valid = '[2016-01-05,)' WHERE id = 1;
UPDATE xxx SET xxx = 'iii', period_valid = '[2016-01-09,)' WHERE id = 1;
UPDATE xxx SET xxx = 'ggg', period_valid = '[2016-01-07,)' WHERE id = 1; -- no effect on xxx
