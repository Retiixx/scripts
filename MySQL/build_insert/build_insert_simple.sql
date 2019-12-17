CREATE
    DEFINER = `campagne`@`%` PROCEDURE `build_insert_simple`(IN v_tablename varchar(100), IN v_columns TEXT,
                                                             IN v_where TEXT, IN v_count_per_iteration INTEGER,
                                                             IN v_i INTEGER, IN v_limit INTEGER,
                                                             IN v_debug INTEGER)

BEGIN

    DECLARE v_sql TEXT;
    DECLARE v_insert TEXT;

    SET v_count_per_iteration = IF(v_count_per_iteration REGEXP '^[0-9]+$' = 1, v_count_per_iteration, 1000);
    SET v_i = IF(v_i REGEXP '^[0-9]+$' = 1, v_i, 0);
    SET v_limit = IF(v_limit REGEXP '^[0-9]+$' = 1, v_limit, 0);

    SET GROUP_CONCAT_MAX_LEN = CAST(pow(2, 31) AS UNSIGNED);

    IF (v_columns = '' OR v_columns = '*') THEN

        SET @v_columns = '';

        SET @sql = CONCAT(
                'SELECT GROUP_CONCAT(CONCAT(''`'', `COLUMN_NAME`, ''`'') ORDER BY `ORDINAL_POSITION` SEPARATOR '', '') INTO @v_columns',
                ' FROM information_schema.COLUMNS',
                ' WHERE TABLE_NAME = ''', v_tablename, '''',
                ' AND GENERATION_EXPRESSION = ''''',
                ' AND EXTRA NOT LIKE ''%auto_increment%'''
            );

        IF v_debug THEN
            SELECT CONCAT('** @sql = ', @sql) AS '** DEBUG :';
        END IF;

        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET v_columns = @v_columns;

    END IF;

    SET v_insert = CONCAT('INSERT INTO `', v_tablename, '` (', v_columns, ') VALUES');
    SET v_sql = CONCAT('SELECT ', v_columns, ' FROM `', v_tablename, '`');

    IF LENGTH(v_where) > 0 THEN
        SET v_sql = CONCAT(v_sql, ' WHERE ', v_where);
    END IF;

    IF v_debug THEN
        SELECT CONCAT('** v_sql = ', v_sql) AS '** DEBUG :';
    END IF;

    CALL build_insert_complex(v_sql, v_insert, v_count_per_iteration, v_i, v_limit, v_debug);

END