-- A minimal reproduce case showing when rollback happens, how will the whole system behave
function thread_init(tid) 
    drv = sysbench.sql.driver()
    con = drv:connect()
end
function event()
    local next_id
    con:query("BEGIN");
    con:query("SELECT * FROM test.customer LIMIT 1"); -- Make it a cross-engine trx, which will also advance the ERMIA lsn for each trx
    next_id = con:query_row("SELECT `next_id` FROM district WHERE `pkey` = 1 FOR UPDATE"); -- It's a EXCLUSIVE LOCK which won't fetch the read view at all
    con:query(([[UPDATE district SET `next_id` = %d WHERE `pkey` = 1]]):format(next_id + 1)) -- UPDATE won't trigger the acquire read view either
    con:query([[SELECT * FROM order_line LIMIT 1]]) -- Now we acquire the innodb read view
    con:query(([[INSERT INTO order_line (`oid`) VALUES(%d)]]):format(next_id)) -- insert into another table
    -- if sysbench.rand.uniform(0, 10) > 8 then
    --     con:query("ROLLBACK")
    --     return
    -- end
    con:query("COMMIT"); -- commit the transaction, the whole trx shouldn't have any problem such as duplicated entry.
end

function prepare()
    print("PREPARE")
    drv = sysbench.sql.driver()
    con = drv:connect()
    con:query([[
CREATE TABLE IF NOT EXISTS customer (
    `id` INT(11) NOT NULL,
        PRIMARY KEY (`id`)
        )ENGINE = ERMIA
    ]])
    con:query([[
CREATE TABLE IF NOT EXISTS district (
    `pkey` INT(11) NOT NULL, 
        `next_id` INT(11) NOT NULL,
            PRIMARY KEY (`pkey`)
            )ENGINE = InnoDB;
    ]])
    con:query([[
    CREATE TABLE IF NOT EXISTS order_line (
        `oid` INT(11) NOT NULL,
            PRIMARY KEY (`oid`)
            )ENGINE = InnoDB;
    ]])
    con:query([[INSERT INTO customer (`id`) VALUES (1)]])
    con:query([[INSERT INTO customer (`id`) VALUES (2)]])
    con:query([[INSERT INTO customer (`id`) VALUES (3)]])
    con:query([[INSERT INTO district (`pkey`, `next_id`) VALUES (1, 1)]])
    print("OK")
end
