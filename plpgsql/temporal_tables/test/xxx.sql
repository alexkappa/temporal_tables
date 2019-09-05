CREATE TABLE xxx
(
    id           INT PRIMARY KEY NOT NULL,
    xxx          text,
    period_valid tstzrange       NOT NULL
);

CREATE TABLE xxx_history (LIKE xxx) PARTITION BY RANGE (lower(period_valid));

CREATE TABLE xxx_history_y2015 PARTITION OF xxx_history
    FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');

CREATE TABLE xxx_history_y2016 PARTITION OF xxx_history
    FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');

CREATE TABLE xxx_history_y2017 PARTITION OF xxx_history
    FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');

DROP TRIGGER IF EXISTS xxx_update_trigger ON xxx;
CREATE TRIGGER xxx_update_trigger
    BEFORE UPDATE ON xxx
    FOR EACH ROW EXECUTE PROCEDURE update_history();

TRUNCATE xxx;
TRUNCATE xxx_history;
INSERT INTO xxx VALUES (1, 'aaa', '[2016-01-01,)');
UPDATE xxx SET xxx = 'bbb', period_valid = '[2016-01-02,)' WHERE id = 1;
UPDATE xxx SET xxx = 'ccc', period_valid = '[2016-01-03,)' WHERE id = 1;
UPDATE xxx SET xxx = 'zzz', period_valid = '[2015-01-01,)' WHERE id = 1;
UPDATE xxx SET xxx = 'yyy', period_valid = '[2015-01-03,)' WHERE id = 1;
UPDATE xxx SET xxx = 'ddd', period_valid = '[2016-01-04,)' WHERE id = 1;
UPDATE xxx SET xxx = 'fff', period_valid = '[2016-01-06,)' WHERE id = 1;
UPDATE xxx SET xxx = 'eee', period_valid = '[2016-01-05,)' WHERE id = 1;
UPDATE xxx SET xxx = 'iii', period_valid = '[2016-01-09,)' WHERE id = 1;
UPDATE xxx SET xxx = 'ggg', period_valid = '[2016-01-07,)' WHERE id = 1;