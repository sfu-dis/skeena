DROP DATABASE IF EXISTS secidx;
CREATE DATABASE secidx;
CREATE TABLE secidx.demo(
    id BIGINT(10) NOT NULL DEFAULT 0,
    name VARCHAR(255) NOT NULL DEFAULT '',
    INDEX idx_nm(`name`)
) ENGINE = ERMIA;

USE secidx;
DELIMITER $$
CREATE PROCEDURE RunTest()
    BEGIN
    DECLARE x INT DEFAULT 0;
    WHILE x < 8192 DO
        INSERT INTO secidx.demo VALUES(1, "VOID001"), (2, "VOID001"), (3, "VOID001"), (4, "VOID001");
        SET x = x + 1;
        SELECT x AS '';
    END WHILE;
END$$
DELIMITER ;

CALL RunTest();
