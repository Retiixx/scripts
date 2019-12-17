CREATE
    DEFINER = `campagne`@`%` PROCEDURE `build_insert_complex`(IN v_query TEXT, IN v_insert TEXT,
                                                              IN v_count_per_iteration INTEGER, IN v_i INTEGER,
                                                              IN v_limit INTEGER, IN v_debug INTEGER)

BEGIN

    -- region === Variables ===
    DECLARE v_tablename VARCHAR(50) DEFAULT 't_temp_insertquerydata';
    DECLARE v_concat LONGTEXT DEFAULT '';

    SET v_count_per_iteration = IF(v_count_per_iteration REGEXP '^[0-9]+$' = 1, v_count_per_iteration, 1000);
    SET v_i = IF(v_i REGEXP '^[0-9]+$' = 1, v_i, 0);
    SET v_limit = IF(v_limit REGEXP '^[0-9]+$' = 1, v_limit, 0);

    -- MAX 32 bits
    SET GROUP_CONCAT_MAX_LEN = CAST(pow(2, 31) AS UNSIGNED);

    SET @v_table_count = 0;
    -- endregion

    -- region === DROP TABLES ===
    SET @sql = CONCAT('DROP TABLE IF EXISTS ', v_tablename, ';');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @sql = CONCAT('DROP TEMPORARY TABLE IF EXISTS ', v_tablename, '_temp;');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    -- endregion

    -- region === CREATE COLUMNS TABLE ===
    -- On crée une table véritable pour obtenir la liste et le nom des colonnes
    -- On extrait de la master table des colonnes le nom des colonnes, or une table temporaire n'apparait jamais dedans

    SET @sql = CONCAT('CREATE TABLE ', v_tablename, ' ENGINE=InnoDB AS SELECT * FROM (', v_query, ')t LIMIT 0;');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    -- endregion

    -- region === CREATE WORK TABLE ===
    -- on crée une table temp pour n'avoir que les lignes concernées
    SET @sql = CONCAT('CREATE TEMPORARY TABLE ', v_tablename, '_temp ENGINE=InnoDB AS ', v_query, ';');
    IF v_limit <> 0 THEN
        SET @sql = CONCAT('CREATE TEMPORARY TABLE ', v_tablename, '_temp ENGINE=InnoDB AS SELECT * FROM (', v_query,
                          ') t LIMIT ', v_i, ', ', v_limit);
    END IF;

    IF v_debug THEN
        SELECT CONCAT('** @sql = ', @sql) AS '** DEBUG :';
    END IF;

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    -- endregion

    DROP TEMPORARY TABLE IF EXISTS t_queries;
    CREATE TEMPORARY TABLE t_queries
    (
        query LONGTEXT
    );

    -- Nb de lignes
    SET @sql = CONCAT('SELECT COUNT(*) INTO @v_table_count FROM `', v_tablename, '_temp`');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    IF v_debug THEN
        SELECT CONCAT('** @v_table_count = ', @v_table_count) AS '** DEBUG :';
    END IF;

    -- region === Récupération du nom des colonnes ===
    -- Création d'une string prête pour un concat à partir de la table vide
    -- Ex: column1, column2, quote(column3)

    SET v_concat = (
        SELECT GROUP_CONCAT(CASE
                                WHEN DATA_TYPE LIKE '%int%' THEN CONCAT('IF(`', `COLUMN_NAME`, '` IS NULL, ''NULL'', `',
                                                                        `COLUMN_NAME`, '`)')
                                ELSE CONCAT('IF(`', `COLUMN_NAME`, '` IS NULL, ''NULL'', QUOTE(`', `COLUMN_NAME`, '`))')
                                END ORDER BY `ORDINAL_POSITION` SEPARATOR ', '','', ')
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = schema()
          AND TABLE_NAME = v_tablename
          AND GENERATION_EXPRESSION = ''
          AND EXTRA NOT LIKE '%auto_increment%'
    );

    SET @sql = CONCAT('DROP TABLE IF EXISTS ', v_tablename, ';');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    -- endregion

    IF v_debug THEN
        SELECT CONCAT('** v_concat = ', v_concat) AS '** DEBUG :';
    END IF;

    -- String magique
    SET @sql = '';
    SET @sql = CONCAT(
            @sql,
            'INSERT INTO t_queries ',
            'SELECT GROUP_CONCAT(',
            'CONCAT(''('', ', v_concat, ','')'')',
            ' SEPARATOR '','')',
            ' FROM (SELECT * FROM ', v_tablename, '_temp'
        );

    SET v_i = 0;
    -- On boucle pour créer les chaines de values, tant qu'on a des lignes dans la table temp
    WHILE v_i < @v_table_count
        DO
            SET @_sql = @sql;
            SET @_sql = CONCAT(
                    @_sql,
                    ' LIMIT ', v_i, ', ', v_count_per_iteration, ') t'
                );

            /*IF v_debug THEN
                SELECT CONCAT('** @_sql = ', @_sql) AS '** DEBUG :';
            END IF;*/

            PREPARE stmt FROM @_sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;

            SET v_i = v_i + v_count_per_iteration;

        END WHILE;

    -- On ajoute la string Insert avec la liste des values

    SELECT CONCAT(v_insert, ' ', `query`, ';') as 'query' FROM t_queries;

END