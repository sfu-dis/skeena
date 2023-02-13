-- create wrapper with handler
CREATE OR REPLACE FUNCTION dummy_handler ()
RETURNS fdw_handler
AS 'dummy_data'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER dummy_data
HANDLER dummy_handler;

