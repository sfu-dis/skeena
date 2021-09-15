DROP DATABASE IF EXISTS secidx;
CREATE DATABASE secidx;
CREATE TABLE secidx.demo(
    id BIGINT(10) NOT NULL DEFAULT 0,
    name VARCHAR(255) NOT NULL DEFAULT '',
    idno VARCHAR(255) UNIQUE NOT NULL,
    ord BIGINT(10) NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (id),
    INDEX idx_nm(`name`),
    INDEX idxmul(`name`, `ord`),
    INDEX idx_org(`ord`)
) ENGINE = ERMIA;

INSERT INTO secidx.demo VALUES(1, "VOID001", "888888888888888", 12); 
INSERT INTO secidx.demo VALUES(2, "VOID001", "777777777777777", 12); 
INSERT INTO secidx.demo VALUES(3, "VOID009", "999999999999999", 19); 
-- TEST1: Query the table with non-unique secondary index ord (NOT VARCHAR)
SELECT * FROM secidx.demo WHERE ord = 12;

SELECT * FROM secidx.demo WHERE name = "VOID001";
SELECT * FROM secidx.demo WHERE name = "VOID000";
-- SELECT * FROM secidx.demo WHERE idxmul = "VOID001";
