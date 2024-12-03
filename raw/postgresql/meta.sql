
CREATE SCHEMA version_meta;

CREATE TABLE version_meta.history(
    version INT NOT NULL,
	update_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE OR REPLACE PROCEDURE version_meta.sp_update_db_version(new_version INT)
LANGUAGE plpgsql
AS $$

BEGIN

    INSERT INTO version_meta.history VALUES (new_version);
END; $$;

CREATE OR REPLACE VIEW version_meta.current_version
	AS (
        SELECT
            h."version"
        FROM version_meta.history h
        ORDER BY h.update_date DESC
        LIMIT 1
    );

CALL version_meta.sp_update_db_version(0);
