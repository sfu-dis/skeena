CREATE DATABASE IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.inno (`key` BIGINT(20), `value` VARCHAR(20));
CREATE TABLE IF NOT EXISTS demo.orz (
  id BIGINT NOT NULL,
  value CHAR(10) NOT NULL,
  PRIMARY KEY (id)
  -- INDEX idx_v(value)
) ENGINE = ERMIA;
CREATE TABLE IF NOT EXISTS demo.inno2 (`key` BIGINT(20), `value` VARCHAR(20));

BEGIN;
INSERT INTO demo.inno(`key`, value) VALUES(110, "qwq");
INSERT INTO demo.orz(`id`, value) VALUES(121, "p");
SELECT * FROM demo.orz;
SELECT * FROM demo.inno;
UPDATE demo.inno SET value = "Persona 5" WHERE `key` = 100;
COMMIT;

