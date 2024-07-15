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
        $this->results = [];
        $this->tables = [];
        $this->exceptions = [];
        $this->errorPlaces = [];
        $this->catchQueries = [];

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
        try {
            $this->connect();
            $this->fetchColumnInformation();
            $this->compareSchemas();
            $this->syncTables();
            $this->printResults();
            $this->handleCatchedQueries();
        } catch (Exception $e) {
            echo 'Error: ' . $e->getMessage();
        }
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
        $columnsQuery1 = $this->bdd1->query("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" . $this->dbname1 . "';");
        $columnsQuery2 = $this->bdd2->query("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" . $this->dbname2 . "';");

        $columns1 = $columnsQuery1->fetchAll(PDO::FETCH_ASSOC);
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
        var_dump($columns);
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
            // var_dump($columns);
            // die;
            $this->createTable($tableName, $columns);
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
     * Creates a new table in the target database
     * 
     * @param string $tableName Name of the table to create
     * @param array $columns Columns of the table
     */
    private function createTable($tableName, $columns)
    {
        if ($this->isView($tableName)) {
            $this->createView($tableName);
        } else {
            $createTableSQL = "CREATE TABLE `$tableName` (";
            $columnDefinitions = [];
            foreach ($columns as $columnName => $columnType) {
                $columnDefinitions[] = "`$columnName` $columnType";
            }
            $createTableSQL .= implode(', ', $columnDefinitions) . ");";
            $this->bdd2->query($createTableSQL);
            echo "\n\e[1;37;43m Table " . $tableName . " created in " . $this->dbname2 . " db\e[0m\n";
        }
    }


    /**
     * Check if the table is a view or not
     *
     * @param  mixed $tableName
     * @return void
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
     * Synchronizes all tables between databases
     */
    private function syncTables()
    {
        foreach ($this->db1Columns as $tableName => $columns) {
            if (!in_array($tableName, $this->tablesIgnore)) {
                $this->syncTable($tableName);
            } else {
                echo "\e[1;37;41m" . $tableName . " not inserted because its depends directly on the project \e[0m\n";
            }
        }
    }

    /**
     * Synchronizes a single table between databases
     * 
     * @param string $tableName Name of the table to synchronize
     */
    private function syncTable($tableName)
    {
        $totalFailed = 0;

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

        echo "\n______________________________________________________________________________________________________________________________________________________________________\n\n";
        foreach ($this->errorPlaces as $value) {
            if ($value["error"] > 0) {
                echo  "❌ " . $value["table"] . " : (" . $value["error"] . " erreur/" . $value["total"] . " Total)\n";
            }
        }
        echo "\n______________________________________________________________________________________________________________________________________________________________________\n";
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
}


$syncer = new DatabaseSyncer('avdb_7', 'test_new', false, false);
$syncer->syncDatabases();
