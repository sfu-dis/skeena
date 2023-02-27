-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION ermia_fdw" to load this file. \quit

CREATE OR REPLACE FUNCTION ermia_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER ermia_fdw
HANDLER ermia_fdw_handler;

