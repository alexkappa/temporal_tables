CREATE OR REPLACE FUNCTION update_history() RETURNS TRIGGER AS
$$
DECLARE
    tx                 txid_snapshot;
    table_name         text := TG_TABLE_NAME;
    table_name_history text := TG_TABLE_NAME || '_history';
    period_valid_lower timestamptz;
    period_valid_upper timestamptz;
BEGIN

    tx := txid_current_snapshot();
    IF OLD.xmin::text >= (txid_snapshot_xmin(tx) % (2 ^ 32)::bigint)::text
        AND OLD.xmin::text <= (txid_snapshot_xmax(tx) % (2 ^ 32)::bigint)::text THEN
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

        EXECUTE 'SELECT MIN(table_with_history.min_lower_period_valid)' ||
                ' FROM (' ||
                '   SELECT MIN(lower(period_valid)) AS min_lower_period_valid' ||
                '   FROM ' || quote_ident(table_name) ||
                '   WHERE id = $1.id AND lower(period_valid) > ' || quote_literal(period_valid_lower) ||
                '   UNION ALL' ||
                '   SELECT MIN(lower(period_valid)) AS min_lower_period_valid' ||
                '   FROM ' || quote_ident(table_name_history) ||
                '   WHERE id = $1.id AND lower(period_valid) > ' || quote_literal(period_valid_lower) ||
                ') AS table_with_history;'
            USING NEW INTO period_valid_upper;

        period_valid_upper := period_valid_upper;

        EXECUTE 'DELETE FROM ' || quote_ident(table_name_history) ||
                ' WHERE id = $1.id ' ||
                ' AND period_valid = tstzrange(' ||
                '   ' || quote_literal(period_valid_lower) || ', ' ||
                '   ' || quote_literal(period_valid_upper) || ', ' ||
                '   ' || quote_literal('[)') ||
                ');'
            USING NEW;

        EXECUTE 'UPDATE ' || quote_ident(table_name_history) ||
                ' SET period_valid = tstzrange(' ||
                '   lower(period_valid),' ||
                '   ' || quote_literal(period_valid_lower) || ', ' ||
                '   ' || quote_literal('[)') ||
                ' )' ||
                ' WHERE id = $1.id ' ||
                ' AND period_valid @> ' || quote_literal(period_valid_lower) || '::timestamptz ;'
            USING NEW;

        NEW.period_valid = tstzrange(period_valid_lower, period_valid_upper, '[)');

        EXECUTE 'INSERT INTO ' || quote_ident(table_name_history) || ' VALUES ($1.*);'
            USING NEW;

        RETURN NULL;

    ELSEIF lower(NEW.period_valid) = lower(OLD.period_valid) THEN

        -- The updated and existing period_valid are the same, therefore we will skip a historical
        -- entry. This is to ensure that a historical record is only updated when the period_valid
        -- changes.

        RETURN NEW;
    ELSE
        -- The updated lower bound of period_valid is later than the existing lower bound
        -- period_valid. This means the record is allowed to update xxx as well as
        -- xxx_history.

        period_valid_lower := lower(OLD.period_valid);
        period_valid_upper := lower(NEW.period_valid);

        OLD.period_valid = tstzrange(period_valid_lower, period_valid_upper, '[)');

        EXECUTE 'INSERT INTO ' || quote_ident(table_name_history) || ' VALUES ($1.*)'
            USING OLD;

        RETURN NEW;
    END IF;
END
$$ LANGUAGE plpgsql;