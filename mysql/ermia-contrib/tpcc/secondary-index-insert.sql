DROP DATABASE IF EXISTS secidx;
CREATE DATABASE secidx;
CREATE TABLE secidx.demo(
    id BIGINT(10) NOT NULL DEFAULT 0,
    name VARCHAR(255) NOT NULL DEFAULT '',
    idno VARCHAR(255) UNIQUE NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_nm(`name`)
) ENGINE = ERMIA;

INSERT INTO secidx.demo VALUES(1, "VOID001", "ID0001101ud"), (2, "VOID001", "ID777777777"), (3, "VOID001", "RDXIDV-10-129");
SELECT 1 \G
---- This should fail (PK DUPP)
-- INSERT INTO secidx.demo VALUES(1, "VOID001", "IDFOOOO");
---- This should also fail (UK DUPP)
-- INSERT INTO secidx.demo VALUES(4, "VOID001", "ID0001101ud");
