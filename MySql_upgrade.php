<?php

class DatabaseSyncer
{
    private $handleProblemInstantinously;
    private $DEBUG_MODE;
    private $bdd1;
    private $bdd2;
    private $dbname1;
    private $dbname2;
    private $tablesIgnore;
    private $results;
    private $tables;
    private $exceptions;
    private $errorPlaces;
    private $catchQueries;
    private $db1Columns;
    private $db2Columns;
    private $failedTables;
    private $relatedTablesToHandle;
    private $relatedTables;
    private $test_duration_rows;
    private $totalTables;
    private $processedTables;
    private $startTime;
    private $lastUpdateTime;

    /**
     * The DatabaseSyncer constructor
     *
     * @param  string $dbname1
     * @param  string $dbname2
     * @param  bool $DEBUG_MODE
     * @param  bool $handleProblemInstantinously
     *
     */
    public function __construct($dbname1, $dbname2, $DEBUG_MODE = false, $handleProblemInstantinously = false)
    {
        $this->dbname1 = $dbname1;
        $this->dbname2 = $dbname2;
        $this->DEBUG_MODE = $DEBUG_MODE;
        $this->handleProblemInstantinously = $handleProblemInstantinously;
        $this->tablesIgnore = [
            "ps_configuration",
            "ps_shop",
            "ps_shop_url",
            "brand_category",
            "brandcategory_product",
            "categoriesofmotoyear",
            "ps_abandoned_cart_amount",
            "ps_appagebuilder",
            "ps_appagebuilder_details",
            "ps_appagebuilder_details_shop",
            "ps_appagebuilder_extracat",
            "ps_appagebuilder_extrapro",
            "ps_appagebuilder_lang",
            "ps_appagebuilder_page",
            "ps_appagebuilder_positions",
            "ps_appagebuilder_positions_shop",
            "ps_appagebuilder_products",
            "ps_appagebuilder_products_shop",
            "ps_appagebuilder_profiles",
            "ps_appagebuilder_profiles_lang",
            "ps_appagebuilder_profiles_shop",
            "ps_appagebuilder_shop",
            "ps_appagebuilder_shortcode",
            "ps_appagebuilder_shortcode_lang",
            "ps_appagebuilder_shortcode_shop",
            "ps_btmegamenu",
            "ps_btmegamenu_group",
            "ps_btmegamenu_group_lang",
            "ps_btmegamenu_lang",
            "ps_btmegamenu_shop",
            "ps_btmegamenu_widgets",
            "ps_caissepos_configuration",
            "ps_caissepos_customer",
            "ps_caissepos_decimalproduct",
            "ps_caissepos_favorites",
            "ps_caissepos_index",
            "ps_caissepos_orders",
            "ps_caissepos_orders_state",
            "ps_caissepos_store_price",
            "ps_caissepos_users",
            "ps_decimal_type",
            "ps_dhl_address",
            "ps_dhl_capital",
            "ps_dhl_commercial_invoice",
            "ps_dhl_error",
            "ps_dhl_error_lang",
            "ps_dhl_extracharge",
            "ps_dhl_extracharge_lang",
            "ps_dhl_label",
            "ps_dhl_order",
            "ps_dhl_package",
            "ps_dhl_pickup",
            "ps_dhl_plt",
            "ps_dhl_service",
            "ps_dhl_service_lang",
            "ps_dhl_shipment_tracking",
            "ps_dhl_tracking",
            "ps_dhl_tracking_lang",
            "ps_kb_checkout_behaviour_stats",
            "ps_kb_supercheckout_gift_message",
            "ps_leoblog_blog",
            "ps_leoblog_blog_lang",
            "ps_leoblog_blog_shop",
            "ps_leoblog_comment",
            "ps_leoblogcat",
            "ps_leoblogcat_lang",
            "ps_leoblogcat_shop",
            "ps_leofeature_compare",
            "ps_leofeature_compare_product",
            "ps_leofeature_product_review",
            "ps_leofeature_product_review_criterion",
            "ps_leofeature_product_review_criterion_category",
            "ps_leofeature_product_review_criterion_lang",
            "ps_leofeature_product_review_criterion_product",
            "ps_leofeature_product_review_grade",
            "ps_leofeature_product_review_report",
            "ps_leofeature_product_review_usefulness",
            "ps_leofeature_wishlist",
            "ps_leofeature_wishlist_product",
            "ps_leopartsfilter_device",
            "ps_leopartsfilter_device_lang",
            "ps_leopartsfilter_device_shop",
            "ps_leopartsfilter_import",
            "ps_leopartsfilter_level5",
            "ps_leopartsfilter_level5_lang",
            "ps_leopartsfilter_level5_shop",
            "ps_leopartsfilter_make",
            "ps_leopartsfilter_make_lang",
            "ps_leopartsfilter_make_shop",
            "ps_leopartsfilter_model",
            "ps_leopartsfilter_model_lang",
            "ps_leopartsfilter_model_shop",
            "ps_leopartsfilter_product",
            "ps_leopartsfilter_year",
            "ps_leopartsfilter_year_lang",
            "ps_leopartsfilter_year_shop",
            "ps_leoslideshow_groups",
            "ps_leoslideshow_slides",
            "ps_leoslideshow_slides_lang",
            "ps_lgcookieslaw",
            "ps_lgcookieslaw_lang",
            "ps_migrationpro_configuration",
            "ps_migrationpro_data",
            "ps_migrationpro_error_logs",
            "ps_migrationpro_mapping",
            "ps_migrationpro_migrated_data",
            "ps_migrationpro_pass",
            "ps_migrationpro_process",
            "ps_migrationpro_save_mapping",
            "ps_migrationpro_warning_logs",
            "ps_paypal_capture",
            "ps_paypal_ipn",
            "ps_paypal_order",
            "ps_paypal_processlogger",
            "ps_paypal_vaulting",
            "ps_stock_bak",
            "ps_velsof_supercheckout_custom_field_options_lang",
            "ps_velsof_supercheckout_custom_fields",
            "ps_velsof_supercheckout_custom_fields_lang",
            "ps_velsof_supercheckout_customer_consent",
            "ps_velsof_supercheckout_fields_data",
            "ps_velsof_supercheckout_policies",
            "ps_velsof_supercheckout_policy_lang",
            "ps_wishlist",
            "ps_wishlist_email",
            "ps_wishlist_product",
            "ps_wishlist_product_cart"
        ];
        $this->relatedTables = [
            "ps_kerawen_525_till_flow" => "ps_kerawen_525_operation",
            "ps_kerawen_525_payment" => "ps_kerawen_525_operation",
            "ps_kerawen_525_sale" => "ps_kerawen_525_operation",
            "ps_kerawen_525_order" => "ps_kerawen_525_sale",
            "ps_kerawen_525_discount" => "ps_kerawen_525_sale",
            "ps_kerawen_525_duplicate" => "ps_kerawen_525_sale",
            "ps_kerawen_525_sale_tax" => "ps_kerawen_525_sale",
            "ps_kerawen_525_sale_detail" => "ps_kerawen_525_order",
            "ps_kerawen_525_invoice" => "ps_kerawen_525_order",
            "ps_kerawen_525_order_tax" => "ps_kerawen_525_order",
            "ps_kerawen_525_gtotal_tax" => "ps_kerawen_525_gtotal"
        ];
        $this->relatedTablesToHandle = [];
        $this->results = [];
        $this->failedTables = [];
        $this->tables = [];
        $this->exceptions = [];
        $this->errorPlaces = [];
        $this->catchQueries = [];
        $this->test_duration_rows = 0;
        $this->totalTables = 0;
        $this->processedTables = 0;

        error_reporting(E_ALL);
        ini_set('display_errors', 1);
    }

    /**
     * Synchronizes databases
     *  - Establishes database connections
     * - Fetches column information from both databases
     * - Compares schemas of both databases
     * - Synchronizes all tables between databases
     * - Prints synchronization results
     * - Handles caught queries, optionally saving them to a file
     * 
     */
    public function syncDatabases()
    {
        $time_pre = microtime(true);
        $this->lastUpdateTime = $this->startTime;
        try {

            $this->connect();
            $this->fetchColumnInformation();
            $this->compareSchemas();
            $this->totalTables = count($this->db1Columns) - count($this->tablesIgnore);
            $this->syncTables();
            !empty($this->failedTables) && $this->reSyncFailedTables();
            !empty($this->relatedTables) && $this->reSyncRelatedTables();
            $this->printResults();
            $this->showFailedTables();

            $time_post = microtime(true);
            $exec_time = $time_post - $time_pre;
            echo "\n⏱️   Execution time: " . number_format($exec_time, 2) . " seconds\n";

            $this->handleCatchedQueries();
        } catch (Exception $e) {
            echo 'Error: ' . $e->getMessage();
        }
        $endTime = microtime(true);
        $executionTime = $endTime - $this->startTime;
        
        echo "\nTotal execution time: " . number_format($executionTime, 2) . " seconds\n";
    }

    /**
     * Establishes database connections
     */
    private function connect()
    {
        $this->bdd1 = new PDO('mysql:host=localhost;dbname=' . $this->dbname1 . ';charset=utf8', "root", "");
        $this->bdd2 = new PDO('mysql:host=localhost;dbname=' . $this->dbname2 . ';charset=utf8', "root", "");
    }

    /**
     * Fetches column information from both databases
     */
    private function fetchColumnInformation()
    {
        $columnsQuery1 = $this->bdd1->query("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" . $this->dbname1 . "' ORDER BY TABLE_NAME, ORDINAL_POSITION;");
        $columnsQuery2 = $this->bdd2->query("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" . $this->dbname2 . "' ORDER BY TABLE_NAME, ORDINAL_POSITION;");

        $columns1 = $columnsQuery1->fetchAll(PDO::FETCH_ASSOC);
        // var_dump($columns1);
        // die;
        $columns2 = $columnsQuery2->fetchAll(PDO::FETCH_ASSOC);

        $this->db1Columns = $this->convertToAssociativeArray($columns1);
        $this->db2Columns = $this->convertToAssociativeArray($columns2);
    }

    /**
     * Converts column information to associative array
     * 
     * @param array $columns Array of column information
     * @return array Associative array of column information
     */
    private function convertToAssociativeArray($columns)
    {
        // var_dump($columns);
        $result = [];
        foreach ($columns as $column) {
            $result[$column['TABLE_NAME']][$column['COLUMN_NAME']] = $column['COLUMN_TYPE'];
        }
        return $result;
    }

    /**
     * Compares schemas of both databases
     */
    private function compareSchemas()
    {
        $identical = true;
        foreach ($this->db1Columns as $tableName => $columns) {
            if (!in_array($tableName, $this->tablesIgnore)) {
                $this->compareTable($tableName, $columns);
            }
        }
        // if ($identical) {
        //     $this->tables[] = "tables are identical";
        //     $this->results[] = "columns are identical";
        // }
    }

    /**
     * Compares a single table between databases
     * 
     * @param string $tableName Name of the table to compare
     * @param array $columns Columns of the table
     * @param bool $identical Reference to boolean indicating if schemas are identical
     */
    private function compareTable($tableName, $columns)
    {
        // var_dump($columns);
        // die;
        if (!isset($this->db2Columns[$tableName])) {
            $this->createTable($tableName);
            // var_dump($tableName);
            // die;
        } else {
            foreach ($columns as $columnName => $columnType) {
                if (!isset($this->db2Columns[$tableName][$columnName])) {
                    // ksgefe
                    // $this->results[] = "Column $columnName added to table $tableName in Database 2 and data copied";

                } elseif ($this->db2Columns[$tableName][$columnName] != $columnType) {
                    $this->exceptions[$this->db2Columns[$tableName][$columnName]] = "table => $tableName column => $columnName type problem";
                }
            }
        }
    }

    /**
     * skip adding column to table when the column is shared between tables
     *
     * @param  mixed $tableName
     * @param  mixed $columnName
     * @return void
     */
    private function skipColumn($tableName, $columnName)
    {
        $this->results[] = "Column $columnName Not added to table $tableName in Database 2 and data not copied";
    }

    /**
     * add column to table when the column is shared between tables
     *
     * @param  mixed $tableName
     * @param  mixed $columnName
     * @param  mixed $columnType
     * @return void
     */
    private function addColumn($tableName, $columnName, $columnType)
    {
        $alterQuery = $this->bdd2->query("ALTER TABLE `$this->dbname2`.`$tableName` ADD COLUMN `$columnName` $columnType");
        $this->bdd2->exec($alterQuery);
        $this->results[] = "Column $columnName added to table $tableName in Database 2 and data copied";
    }

    /**
     * add column into a diffrent table in db2 when the column is shared between tables
     *
     * @param  mixed $tableName
     * @param  mixed $columnName
     * @param  mixed $columnType
     * @return void
     */
    private function otherTableAddColumn($tableName, $columnName, $columnType)
    {
        $alterQuery = $this->bdd2->query("ALTER TABLE `$this->dbname2`.`$tableName` ADD COLUMN `$columnName` $columnType");
        $this->bdd2->exec($alterQuery);
        $this->results[] = "Column $columnName added to table $tableName in Database 2 and data copied";
    }

    /**
     * Creates a new table in the target database
     * 
     * @param string $tableName Name of the table to create
     * @param array $columns Columns of the table
     */
    private function createTable($tableName)
    {
        if ($this->isView($tableName)) {
            return $this->createView($tableName);
        }

        // Get the CREATE TABLE statement from the source database
        $showCreateStmt = $this->bdd1->prepare("SHOW CREATE TABLE `$tableName`");
        $showCreateStmt->execute();
        $createTableSql = $showCreateStmt->fetchColumn(1);

        // Modify the CREATE TABLE statement to fix invalid default values
        $createTableSql = preg_replace(
            [
                "/DEFAULT '0000-00-00 00:00:00'/i",
                "/DEFAULT '0000-00-00'/i"
            ],
            "DEFAULT CURRENT_TIMESTAMP",
            $createTableSql
        );

        // Create the table in the target database
        try {
            $this->bdd2->exec($createTableSql);
            echo "\n\e[1;37;43m Table $tableName created in {$this->dbname2} db\e[0m\n";
        } catch (PDOException $e) {
            echo "\n\e[1;37;41m Error creating table $tableName: " . $e->getMessage() . "\e[0m\n";
            var_dump($tableName, $e->errorInfo);
        }
    }

    /**
     * Check if the table is a view or not
     *
     * @param  string $tableName
     * @return bool
     */
    private function isView($tableName)
    {
        $query = "SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        $stmt = $this->bdd1->prepare($query);
        $stmt->execute([$this->dbname1, $tableName]);
        $result = $stmt->fetch(PDO::FETCH_ASSOC);
        return $result['TABLE_TYPE'] === 'VIEW';
    }

    /**
     * create view in the target database if the table is a view
     *
     * @param  mixed $viewName
     * @return void
     */
    private function createView($viewName)
    {
        $query = "SHOW CREATE VIEW `$this->dbname1`.`$viewName`";
        $stmt = $this->bdd1->query($query);
        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        if ($result) {
            $createViewSQL = $result['Create View'];
            // Replace the original database name with the new one
            $createViewSQL = str_replace("`$this->dbname1`", "`$this->dbname2`", $createViewSQL);

            $this->bdd2->query($createViewSQL);
            echo "\n\e[1;37;43m View " . $viewName . " created in " . $this->dbname2 . " db\e[0m\n";
        } else {
            echo "\n\e[1;37;41m Failed to create view " . $viewName . " in " . $this->dbname2 . " db\e[0m\n";
        }
    }

    /**
     * check if the table exists in the target database
     *
     * @param  object $db
     * @param  string $tableName
     * @return bool
     */
    private function tableExist($db, $tableName): bool
    {
        $stmt = $db->prepare("SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = '" . $this->dbname2 . "' AND table_name = '" . $tableName . "');");
        $stmt->execute();
        return $stmt->fetchColumn();
    }

    /**
     * Synchronizes all tables between databases
     */
    private function syncTables()
    {
        foreach ($this->db1Columns as $tableName => $columns) {
            if (!in_array($tableName, $this->tablesIgnore)) {
                if (array_key_exists($tableName, $this->relatedTables)) {

                    $isExist = $this->tableExist($this->bdd2, $this->relatedTables[$tableName]);
                    if (!$isExist) {
                        $this->createTable($this->relatedTables[$tableName]);
                        $this->syncTable($this->relatedTables[$tableName]);
                        array_push($this->relatedTablesToHandle, $tableName);
                        continue;
                    }
                    $this->syncTable($tableName);
                }
                $this->syncTable($tableName);
                $this->processedTables++;
                $this->displayProgress();
            } else {
                echo "\e[1;37;41m" . $tableName . " not inserted because its depends directly on the project \e[0m\n";
            }
        }
    }

    private function displayProgress()
    {
        $currentTime = microtime(true);
        $progress = ($this->processedTables / $this->totalTables) * 100;
        $estimatedTimeRemaining = $this->estimateRemainingTime($currentTime);
        
        echo sprintf("\rProgress: %.2f%% - Estimated time remaining: %s", 
            $progress, 
            $estimatedTimeRemaining
        );
        
        $this->lastUpdateTime = $currentTime;
    }


    private function estimateRemainingTime($currentTime)
    {
        if ($this->processedTables == 0) {
            return "Calculating...";
        }

        $elapsedTime = $currentTime - $this->startTime;
        $timePerTable = $elapsedTime / $this->processedTables;
        $remainingTables = $this->totalTables - $this->processedTables;
        $remainingTime = $timePerTable * $remainingTables;

        // Adjust estimation based on recent progress
        $recentTimePerTable = ($currentTime - $this->lastUpdateTime) / 1;
        $adjustedRemainingTime = ($remainingTime + $recentTimePerTable * $remainingTables) / 2;
        
        if ($adjustedRemainingTime < 60) {
            return sprintf("%.0f seconds", $adjustedRemainingTime);
        } elseif ($adjustedRemainingTime < 3600) {
            return sprintf("%.1f minutes", $adjustedRemainingTime / 60);
        } else {
            return sprintf("%.1f hours", $adjustedRemainingTime / 3600);
        }
    }


    /**
     * Synchronizes a single table between databases
     * 
     * @param string $tableName Name of the table to synchronize
     */
    private function syncTable($tableName)
    {
        try {
            $totalFailed = 0;
            $stmt = $this->bdd2->prepare("SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = '" . $this->dbname2 . "' AND table_name = '" . $tableName . "');");
            $stmt->execute();
            if (!$stmt->fetchColumn()) {

                echo "\nTable $tableName does not exist in target database. Skipping.\n";
                // Skip to the next table
                $this->failedTables[] = $tableName;
                return;
            }

            $truncateQuery = "TRUNCATE TABLE `$this->dbname2`.`$tableName`";
            $this->bdd2->query($truncateQuery);


            $columnsToInsert = $this->getColumnsToInsert($tableName);
            $columnList = $this->getColumnList($columnsToInsert);
            $placeholders = implode(', ', array_fill(0, count($columnsToInsert), '?'));
            $selectQuery = $this->bdd1->query("SELECT $columnList FROM `$this->dbname1`.`$tableName`");
            $insertStmt = $this->bdd2->prepare("INSERT INTO `$this->dbname2`.`$tableName` ($columnList) VALUES ($placeholders)");

            while ($row = $selectQuery->fetch(PDO::FETCH_ASSOC)) {
                $this->handleDateProblem($row);
                $sqlUnbinded = $this->getSqlUnbinded($tableName, $columnList, $row);
                $totalFailed += $this->insertRow($insertStmt, $row, $tableName, $sqlUnbinded);
            }

            if ($this->DEBUG_MODE) echo "\e[1;37;42m Inserted row in table $tableName\e[0m\n";

            $this->ErrorPlaces($tableName, $totalFailed);
        } catch (PDOException $e) {
            echo "\n\e[1;37;41mError syncing table $tableName: " . $e->getMessage() . "\e[0m\n";
            // db ghadi tskipa
            array_push($this->failedTables, $tableName);
            return;
        }
    }

    /**
     * function to re Sync Failed Tables
     *
     * @return void
     */
    private function reSyncFailedTables()
    {
        foreach ($this->failedTables as $tableName) {
            $this->compareTable($tableName, $this->db1Columns[$tableName]);
        }
        $this->failedTables = [];
    }

    /**
     * function to re Sync Related Tables
     *
     * @return void
     */
    private function reSyncRelatedTables()
    {
        foreach ($this->relatedTablesToHandle as $name) {
            $this->compareTable($name, $this->db1Columns[$name]);
        }
        $this->relatedTablesToHandle = [];
    }

    /**
     * Gets columns to insert for a given table
     * 
     * @param string $tableName Name of the table
     * @return array Array of column names
     */
    private function getColumnsToInsert($tableName)
    {
        $sql2 = $this->bdd2->query("SHOW COLUMNS FROM `" . $tableName . "`");
        if (!$sql2) {
            echo "____________________________________________________________________";
            var_dump($this->bdd2->errorInfo());
            echo "____________________________________________________________________";
            die;
        }
        $db2Columns = $sql2->fetchAll(PDO::FETCH_COLUMN);

        // array_intersect returns the matches between two arrays
        // array_keys returns the keys of an array
        return array_intersect($db2Columns, array_keys($this->db1Columns[$tableName]));
    }

    /**
     * Generates a comma-separated list of column names
     * 
     * @param array $columnsToInsert Array of column names
     * @return string Comma-separated list of column names
     */
    private function getColumnList($columnsToInsert)
    {
        return implode(', ', array_map(function ($col) {
            return "`$col`";
        }, $columnsToInsert));
    }

    /**
     * Handles date problems in row data
     * 
     * @param array &$row Reference to row data
     */
    private function handleDateProblem(&$row)
    {
        foreach ($row as $key => $value) {
            if ($value == "0000-00-00 00:00:00" || $value == "0000-00-00") {
                $row[$key] = (strlen($value) == 19) ? '1970-01-01 00:00:00' : '1970-01-01';
            }
        }
    }

    /**
     * Generates an unbinded SQL insert statement
     * 
     * @param string $tableName Name of the table
     * @param string $columnList Comma-separated list of column names
     * @param array $row Row data
     * @return string Unbinded SQL insert statement
     */
    private function getSqlUnbinded($tableName, $columnList, $row)
    {
        $rowValues = implode(", ", array_map(function ($col) {
            return $col;
        }, $row));
        return "INSERT INTO `$this->dbname2`.`$tableName` ($columnList) VALUES ($rowValues)";
    }

    /**
     * Inserts a row into the target database
     * 
     * @param PDOStatement $insertStmt Prepared statement for insertion
     * @param array $row Row data to insert
     * @param string $tableName Name of the table
     * @param string $sqlUnbinded Unbinded SQL insert statement
     * @return int Number of failed insertions (0 or 1)
     */
    private function insertRow($insertStmt, $row, $tableName, $sqlUnbinded)
    {
        try {
            // array_intersect_key compare the keys of two arrays and returns the matches
            // array_flip flips the keys and values of an array
            $valuesToInsert = array_intersect_key($row, array_flip($this->getColumnsToInsert($tableName)));
            $isInserted = $insertStmt->execute(array_values($valuesToInsert));
            if (!$isInserted) {
                if ($this->handleProblemInstantinously) {
                    return $this->handleInsertionProblem($tableName, $sqlUnbinded);
                } else {
                    $sqlException = $insertStmt->errorInfo();
                    $this->catchQueries[] = ["table" => $tableName, "query" => $sqlUnbinded, "exception" => $sqlException[2]];
                    return 1;
                }
            }
        } catch (PDOException $e) {
            echo "Error inserting row in table $tableName: " . $e->getMessage() . "\n";
            return 1;
        }
        return 0;
    }

    /**
     * Handles insertion problems interactively
     * 
     * @param string $tableName Name of the table
     * @param string $sqlUnbinded Unbinded SQL insert statement
     * @return int Number of failed insertions (0 or 1)
     */
    private function handleInsertionProblem($tableName, $sqlUnbinded)
    {
        echo "\n" . $sqlUnbinded . ";\n";
        echo "\e[1;37;43m query show problems, 1-fix ,else to collect : \e[0m\n";
        $check = trim(fgets(STDIN));

        if (ctype_digit(intval($check)) && intval($check) == 1) {
            echo "\e[1;37;43m Enter Fixed query : \e[0m\n";
            $line = trim(fgets(STDIN));
            $secondChance = $this->bdd2->prepare($line);
            $secondChance->execute();
            return 0;
        } else {
            $this->catchQueries[] = ["table" => $tableName, "query" => $sqlUnbinded];
            return 1;
        }
    }

    /**
     * Records error information for a table
     * 
     * @param string $tableName Name of the table
     * @param int $totalFailed Total number of failed insertions
     */
    private function ErrorPlaces($tableName, $totalFailed)
    {
        $count = $this->bdd1->query("SELECT COUNT(*) FROM `" . $tableName . "`")->fetch();
        $this->errorPlaces[] = ["table" => $tableName, "total" => $count[0], "error" => $totalFailed];
    }

    /**
     * Prints synchronization results
     */
    private function printResults()
    {
        foreach ($this->tables as $line) {
            echo $line . "\n";
        }
        foreach ($this->results as $line) {
            echo $line . "\n";
        }
        if (!empty($this->exceptions)) {
            foreach ($this->exceptions as $line => $value) {
                echo "\n\e[1;37;41m" . "Column /" . $line . "/ : " . $value . "\e[0m\n";
            }
        }

        if (!empty($this->errorPlaces)) {
            echo "\n______________________________________________________________________________________________________________________________________________________________________\n\n";
            foreach ($this->errorPlaces as $value) {
                if ($value["error"] > 0) {
                    echo  "❌ " . $value["table"] . " : (" . $value["error"] . " erreur/" . $value["total"] . " Total)\n";
                }
            }
            echo "\n______________________________________________________________________________________________________________________________________________________________________\n";
        }
    }

    /**
     * Handles caught queries, optionally saving them to a file
     */
    private function handleCatchedQueries()
    {
        echo "\n1- create file for catched queries ,else to skip: \n\n";
        $v = trim(fgets(STDIN));
        if ($v == 1) {
            if (!empty($this->catchQueries)) {
                $output = '';
                foreach ($this->catchQueries as $query) {
                    $output .= "Table: " . $query["table"] . "\n";
                    $output .= "Query: " . $query["query"] . ";\n\n";
                    $output .= "Exception: " . ($query["exception"] ?? '') . ";\n\n";
                    $output .= "\n_____________________________________________________________________________________________________\n";
                }
                file_put_contents("queriesCached.txt", $output);
                echo "Queries have been saved to queriesCached.txt\n";
            } else {
                echo "No queries to save.\n";
            }
        }
        echo "program out";
    }

    /**
     * show failed tables to sync
     *
     * @return void
     */
    private function showFailedTables()
    {
        if (!empty($this->failedTables)) {
            echo "\n______________________________________________________________________________________________________________________________________________________________________\n\n";
            foreach ($this->failedTables as $table) {
                echo  "❌ " . $table . " : failed to sync\n";
            }
            echo "\n______________________________________________________________________________________________________________________________________________________________________\n";
        }
    }
}

$syncer = new DatabaseSyncer('avdb_new', 'test_new', false, false);
$syncer->syncDatabases();
