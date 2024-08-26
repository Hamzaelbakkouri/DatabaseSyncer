<?php
# sket chwiya 🪄

/**
 * DatabaseSyncer is a PHP script that synchronizes two MySQL databases by copying missing tables and columns from one database to another and copying missing data from one database to another
 */
class DatabaseSyncer
{
    private $handleProblemInstantinously;
    private $DEBUG_MODE;
    private $SECOND_PHASE;
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
    private $rowNum;
    private $totalRows;
    private $processedRows;
    private $startTime;
    private $lastUpdateTime;
    private $allRowsDuration;
    private $time_pre;
    private $end_time;
    private $columsMissedInDB1;
    private $columsMissedInDB2;
    private $defaultValues;
    private $columnValuesIgnored;
    private $rowCount;
    private $batchs;
    private $errorBatchs;
    private $incrementRows;
    private $previousTable;
    private $largeContentTables;
    private $ToSwitchValues;
    private $primarykeys;
    private $tableStats;
    /**
     * The DatabaseSyncer constructor
     *
     * @param  string $dbname1
     * @param  string $dbname2
     * @param  bool $DEBUG_MODE
     * @param  bool $handleProblemInstantinously
     * @param  bool $secondPhase
     *
     */
    public function __construct($dbname1, $dbname2, $DEBUG_MODE = false, $handleProblemInstantinously = false, $secondPhase = false)
    {
        $this->dbname1 = $dbname1;
        $this->dbname2 = $dbname2;
        $this->DEBUG_MODE = $DEBUG_MODE;
        $this->handleProblemInstantinously = $handleProblemInstantinously;
        $this->tablesIgnore = [
            "ps_configuration",
            "ps_connections",
            "ps_guest",
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
        $this->totalRows = 0;
        $this->processedRows = 0;
        $this->rowNum = 0;
        $this->allRowsDuration = 0;
        $this->columsMissedInDB1 = [
            "ps_category" => ["referenceallshop",    "referencerspro",    "refrenceavdb"],
            "ps_category_lang" => ["additional_description"],
            "ps_customer_message" => ["allfile_name"],
            "ps_customer_session" => [
                "date_add",
                "date_upd"
            ],
            "ps_employee" => ["has_enabled_gravatar"],
            "ps_employee_session" => [
                "date_add",
                "date_upd"
            ],
            "ps_group" => ["regletaxe"],
            "ps_hook" => ["active"],
            "ps_layered_category" => ["controller"],
            "ps_link_block_shop" => ["position"],
            "ps_log" => ["id_lang", "id_shop", "id_shop_group", "in_all_shops"],
            "ps_order_payment" => ["id_employee"],
            "ps_orders" => ["note"],
            "ps_product" => [
                "colors",
                "notes",
                "product_type",
                "unit_price"
            ],
            "ps_product_shop" => "unit_price",
            "ps_pscheckout_cart" => "environment",
            "ps_shop" => "color",
            "ps_shop_group" => "color",
            "ps_tab" => ["wording", "wording_domain"],
            "ps_up2pay_transaction" => ["id_cart", "ipn"],
            "ps_warehouse" => ["active"],
            "ps_wishlist" => ["is_default"],
        ];
        $this->columsMissedInDB2 = [
            "ps_carrier" => "id_tax_rules_group",
            "ps_psreassurance" => "id_shop",
            "ps_psreassurance_lang" => "id_shop",
            "ps_tab" => "hide_host_mode"
        ];
        $this->defaultValues =
            [
                "datetime" => "1970-01-01 00:00:00",
                "date" => "1970-01-01",
                "int" => 0,
                "float" => 0.0,
                "varchar" => "",
                "text" => "",
                "tinyint" => 0,
                "smallint" => 0,
                "mediumint" => 0,
                "bigint" => 0,
                "decimal" => 0.0,
                "double" => 0.0,
                "char" => "",
                "enum" => "",
                "set" => "",
                "timestamp" => "1970-01-01 00:00:00",
                "time" => "00:00:00",
                "year" => "1970",
                "bit" => 0,
                "binary" => "",
                "varbinary" => "",
                "tinyblob" => "",
                "mediumblob" => "",
                "longblob" => "",
                "blob" => "",
                "tinytext" => "",
                "mediumtext" => "",
                "longtext" => "",
                "json" => "",
                "geometry" => "",
                "point" => "",
                "linestring" => "",
                "polygon" => "",
                "multipoint" => "",
                "multilinestring" => "",
                "multipolygon" => "",
                "geometrycollection" => ""
            ];
        $this->columnValuesIgnored =
            [
                "ps_psreassurance_lang" => ["column" => "id_shop", "value" => 0],
                // "ebay_log" => ["column" => "state", "value" => 0]
            ];
        $this->batchs = [];
        $this->errorBatchs = [];
        $this->rowCount = 1000;
        $this->incrementRows = 0;
        $this->previousTable = 'ebay_images_romain_do_not_remove';
        $this->SECOND_PHASE = $secondPhase;
        $this->primarykeys = [];
        $this->tableStats = [];
        // $this->ToSwitchValues = [
        //     ["table" => "ps_amazon_configuration", "column" => "id_shop_group", "request" => "''", "response" => null],
        //     ["table" => "ps_amazon_configuration", "column" => "id_shop", "request" => "''", "response" => null],
        //     ["table" => "ps_cart_kerawen", "column" => "delivery_date", "request" => "''", "response" => null],
        //     ["table" => "ps_cart_kerawen", "column" => "id_address", "request" => "''", "response" => null],
        //     ["table" => "ps_cart_kerawen", "column" => "count", "request" => "''", "response" => null],
        //     ["table" => "ps_cart_product_kerawen", "column" => "id_tag", "request" => "''", "response" => null],
        //     ["table" => "ps_cart_rule_kerawen", "column" => "id_cart", "request" => "''", "response" => null],
        //     ["table" => "ps_cashdrawer_flow_kerawen", "column" => "id_payment_mode", "request" => "''", "response" => null],
        //     ["table" => "ps_cashdrawer_flow_kerawen", "column" => "id_order_slip", "request" => "''", "response" => null],
        //     ["table" => "ps_cashdrawer_flow_kerawen", "column" => "count", "request" => "''", "response" => null],
        //     ["table" => "ps_cashdrawer_op_kerawen", "column" => "id_employee", "request" => "''", "response" => null],
        //     ["table" => "ps_cashdrawer_sale_kerawen", "column" => "id_order_slip", "request" => "''", "response" => null],
        //     ["table" => "ps_colissimo_label", "column" => "insurance", "request" => "''", "response" => null],
        //     ["table" => "ps_configuration_kpi", "column" => "id_shop_group", "request" => "''", "response" => null],
        //     ["table" => "ps_configuration_kpi", "column" => "id_shop", "request" => "''", "response" => null],
        //     ["table" => "ps_configuration_lang", "column" => "date_upd", "request" => "''", "response" => null],
        //     ["table" => "ps_configurator3d", "column" => "symetric_product", "request" => "''", "response" => null],
        //     ["table" => "ps_customer_kerawen", "column" => "id_prepaid", "request" => "''", "response" => null],
        //     ["table" => "ps_cart_kerawen", "column" => "quote_expiry", "request" => "''", "response" => null],
        //     ["table" => "ps_kerawen_525_payment", "column" => "id_mode", "request" => "''", "response" => null],
        //     ["table" => "ps_kerawen_525_sale_detail", "column" => "id_carrier", "request" => "''", "response" => null],
        //     ["table" => "ps_cashdrawer_op_kerawen", "column" => "error", "request" => "''", "response" => null],
        //     ["table" => "ps_employee", "column" => "optin", "request" => "''", "response" => null],
        //     ["table" => "ps_delivery", "column" => "id_range_price", "request" => "''", "response" => null],
        //     ["table" => "ps_image", "column" => "cover", "request" => "''", "response" => null],
        //     ["table" => "ps_image_shop", "column" => "cover", "request" => "''", "response" => null],
        //     ["table" => "ps_kerawen_525_discount", "column" => "discount_percent", "request" => "''", "response" => null],
        //     ["table" => "ps_marketplace_order_items", "column" => "item_status", "request" => "''", "response" => null],
        //     ["table" => "ps_marketplace_product_action", "column" => "id_product_attribute", "request" => "''", "response" => null],
        //     ["table" => "ps_order_detail_kerawen", "column" => "measure", "request" => "''", "response" => null],
        //     ["table" => "ps_order_kerawen", "column" => "display_date", "request" => "''", "response" => null],
        //     ["table" => "ps_order_kerawen", "column" => "quote_number", "request" => "''", "response" => null],
        //     ["table" => "ps_kerawen_525_invoice", "column" => "invoice_date", "request" => "''", "response" => null],
        //     ["table" => "ps_kerawen_525_payment", "column" => "remain", "request" => "''", "response" => null],
        //     ["table" => "ps_cart_kerawen", "column" => "quote_number", "request" => "''", "response" => null],
        //     ["table" => "ps_cart_rule_kerawen", "column" => "id_product", "request" => "''", "response" => null],
        //     ["table" => "ps_cashdrawer_flow_kerawen", "column" => "id_credit", "request" => "''", "response" => null],
        //     ["table" => "ps_employee", "column" => "last_connection_date", "request" => "''", "response" => null],
        //     ["table" => "ps_ganalytics", "column" => "refund_sent", "request" => "''", "response" => null],
        //     ["table" => "ps_kerawen_525_event", "column" => "id_operator", "request" => "''", "response" => null],
        //     ["table" => "ps_kerawen_525_payment", "column" => "deferred", "request" => "''", "response" => null],
        //     ["table" => "ps_kerawen_525_sale_detail", "column" => "wrapping", "request" => "''", "response" => null],
        //     ["table" => "ps_kerawen_525_till_check", "column" => "id_currency", "request" => "''", "response" => null],
        //     ["table" => "ps_marketplace_order_items", "column" => "id_order_detail", "request" => "''", "response" => null],
        //     ["table" => "ps_marketplace_product_action", "column" => "date_upd", "request" => "''", "response" => null],
        //     ["table" => "ps_of_shipping_rule", "column" => "date_from", "request" => "''", "response" => null],
        //     ["table" => "ps_order_detail_kerawen", "column" => "precision", "request" => "''", "response" => null],
        //     ["table" => "ps_order_kerawen", "column" => "id_till", "request" => "''", "response" => null],
        //     ["table" => "ps_page", "column" => "id_object", "request" => "''", "response" => null],
        //     ["table" => "ps_posmegamenu_submenu", "column" => "submenu_width", "request" => "''", "response" => null],
        //     ["table" => "ps_product", "column" => "low_stock_threshold", "request" => "''", "response" => null],
        //     ["table" => "ps_product_attribute", "column" => "low_stock_threshold", "request" => "''", "response" => null],
        //     ["table" => "ps_product_attribute", "column" => "default_on", "request" => "''", "response" => null],
        //     ["table" => "ps_product_attribute_kerawen", "column" => "measure", "request" => "''", "response" => null],
        //     ["table" => "ps_product_attribute_shop", "column" => "low_stock_threshold", "request" => "''", "response" => null],
        //     ["table" => "ps_product_attribute_shop", "column" => "default_on", "request" => "''", "response" => null],
        //     ["table" => "ps_product_shop", "column" => "low_stock_threshold", "request" => "''", "response" => null],
        //     ["table" => "ps_product_wm_kerawen", "column" => "unit_price", "request" => "''", "response" => null],
        //     ["table" => "ps_pscheckout_cart", "column" => "paypal_authorization_expire", "request" => "''", "response" => null],
        // ];


        error_reporting(E_ALL);
        ini_set('display_errors', 1);
    }

    /**
     * Synchronizes databases
     * - Establishes database connections
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
            $this->totalRows = $this->countTotalRows();
            $this->syncTables();
            $this->reSyncRelatedTables();
            $this->reSyncFailedTables();
            $this->printResults();
            $this->handleTableStats();
            $this->showFailedTables();

            $time_post = microtime(true);
            $exec_time = $time_post - $time_pre;
            echo "\n⏱️   Execution time: " . number_format($exec_time, 2) . " seconds\n";

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
        $columnsQuery1 = $this->bdd1->query("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" . $this->dbname1 . "' ORDER BY TABLE_NAME, ORDINAL_POSITION;");
        $columnsQuery2 = $this->bdd2->query("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" . $this->dbname2 . "' ORDER BY TABLE_NAME, ORDINAL_POSITION;");

        $columns1 = $columnsQuery1->fetchAll(PDO::FETCH_ASSOC);
        $columns2 = $columnsQuery2->fetchAll(PDO::FETCH_ASSOC);

        $this->db1Columns = $this->convertToAssociativeArray($columns1);
        $this->db2Columns = $this->convertToAssociativeArray($columns2);
    }

    /**
     * Count total rows in the source database
     *
     * @return number
     */
    private function countTotalRows()
    {
        $total = 0;
        foreach ($this->db1Columns as $tableName => $columns) {
            if (!in_array($tableName, $this->tablesIgnore) && !in_array($tableName, $this->relatedTables)) {
                $stmt = $this->bdd1->prepare("SELECT COUNT(*) FROM `$tableName`");
                $stmt->execute();
                $total += $stmt->fetchColumn();
            }
        }
        return $total + count($this->relatedTablesToHandle);
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
        if (!isset($this->db2Columns[$tableName])) {
            $this->createTable($tableName);
        } else {
            foreach ($columns as $columnName => $columnType) {
                $column1 = "";
                $column2 = "";
                $sql = "";

                if (strpos($this->db1Columns[$tableName][$columnName], "varchar") !== false) {
                    $column1 = $this->db1Columns[$tableName][$columnName];
                    $column2 = $this->db2Columns[$tableName][$columnName];
                    if (!$this->isSameSize($column1, $column2)) {
                        $this->bdd2->query("ALTER TABLE `$this->dbname2`.`$tableName` MODIFY COLUMN `$columnName` $column1");
                    }
                }

                if (!isset($this->db2Columns[$tableName][$columnName])) {
                    echo "the column " . $columnName . " must be handled manually in table : " . $tableName . "\n";
                } elseif ($this->db2Columns[$tableName][$columnName] != $columnType) {
                    $this->exceptions[$this->db2Columns[$tableName][$columnName]] = "table => $tableName column => $columnName type problem";
                }
            }
        }
    }

    /**
     * function to check if the size of the column is the same in both databases
     *
     * @param  mixed $value1
     * @param  mixed $value2
     * @return bool
     */
    public function isSameSize($value1, $value2)
    {
        $value1 = strstr($value1, "(");
        $value1 = str_replace("(", "", $value1);
        $value1 = str_replace(")", "", $value1);
        $value1 = intval($value1);

        $value2 = strstr($value2, "(");
        $value2 = str_replace("(", "", $value2);
        $value2 = str_replace(")", "", $value2);
        $value2 = intval($value2);

        if ($value1 > $value2) {
            return false;
        }
        return true;
    }

    /**+
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

        $showCreateStmt = $this->bdd1->prepare("SHOW CREATE TABLE `$tableName`");
        $showCreateStmt->execute();
        $createTableSql = $showCreateStmt->fetchColumn(1);

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
        $this->time_pre = microtime(true);

        foreach ($this->db1Columns as $tableName => $columns) {
            if (!in_array($tableName, $this->tablesIgnore) && !in_array($tableName, $this->relatedTables)) {
                if (array_key_exists($tableName, $this->relatedTables)) {
                    $isExist = $this->tableExist($this->bdd2, $this->relatedTables[$tableName]);
                    if (!$isExist) {
                        $this->createTable($this->relatedTables[$tableName]);
                        $this->syncTable($this->relatedTables[$tableName]);
                        continue;
                    }
                    $this->syncTable($tableName);
                }
                $this->syncTable($tableName);
            } else {
                if ($this->DEBUG_MODE) echo "\n\e[1;37;41m" . $tableName . " not inserted because its depends directly on the project \e[0m\n";
            }
        }
    }

    /**
     * collect primary keys of all tables
     * @param string db
     * @return bool
     */
    // public function collectPKS($db) {
    //     $stmt = $this->bdd2->prepare("
    //     SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE 
    //     FROM INFORMATION_SCHEMA.COLUMNS 
    //     WHERE TABLE_SCHEMA = $db AND COLUMN_KEY = 'PRI' 
    //     ORDER BY TABLE_NAME
    // ");

    //     $stmt->execute();
    //     $this->primarykeys = $stmt->fetchAll(PDO::FETCH_ASSOC);

    //     return !empty($this->primarykeys);
    // }

    /**
     * Synchronizes a single table between databases
     * 
     * @param string $tableName Name of the table to synchronize
     */
    private function syncTable($tableName)
    {
        try {
            $totalFailed = 0;
            $count = 0;

            $stmt = $this->bdd2->prepare("SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = '" . $this->dbname2 . "' AND table_name = '" . $tableName . "');");
            $stmt->execute();
            if (!$stmt->fetchColumn()) {
                echo "\nTable $tableName does not exist in target database. Skipping.\n";
                // Skip to the next table
                $this->failedTables[] = $tableName;
                return;
            }

            if (!$this->SECOND_PHASE) {
                $truncateQuery = "TRUNCATE TABLE `$this->dbname2`.`$tableName`";
                $this->bdd2->query($truncateQuery);
            }

            $columnsToInsert = $this->getColumnsToInsert($tableName);


            $columnList = $this->getColumnList($columnsToInsert);

            $placeholders = implode(', ', array_fill(0, count($columnsToInsert), '?'));
            $selectQuery = $this->bdd1->query("SELECT $columnList FROM `$this->dbname1`.`$tableName`");


            $missedColumns = $this->giveValuesForNewColumns($tableName);
            $newColumns = [];
            $newValue = [];
            !empty($missedColumns['cols']) && $newColumns = $missedColumns['cols'];
            !empty($missedColumns['values']) && $newValue = $missedColumns['values'];

            $insertStmt = $this->bdd2->prepare("INSERT INTO `$this->dbname2`.`$tableName` ($columnList " . !empty($newColumns) &&  ", " . $this->getColumnList($newColumns) . ") VALUES ($placeholders " . !empty($newValue) &&  ", " . $this->getValuesList($newValue) . ")");

            // map on the result of the select query to sync it in the target database
            while ($row = $selectQuery->fetch(PDO::FETCH_ASSOC)) {
                if ($this->SECOND_PHASE) {
                    $existingRow = $this->getExistingRow($tableName, $row);
                    if ($existingRow && $this->isIdenticalData($tableName, $row, $existingRow)) {
                        continue;
                    }
                }

                $tableName != $this->previousTable && $count = 0;
                if (!empty($missedColumns['cols']) && !empty($missedColumns['values'])) {
                    foreach ($newColumns as $key => $value) {
                        $row[$value] = $newValue[$key];
                    }
                }

                $this->rowNum++;
                $this->handleDateProblem($row);
                $allString = $columnList . $this->getColumnList($newColumns);
                $sqlUnbinded = $this->getSqlUnbinded($tableName, $allString, $row);


                if (in_array($tableName, array_keys($this->columnValuesIgnored))) {
                    $getOneRowWithCondition = $this->bdd1->query("SELECT * FROM `$tableName` WHERE " . $this->columnValuesIgnored[$tableName]["column"] . " != " . $this->columnValuesIgnored[$tableName]["value"])->fetchAll(PDO::FETCH_ASSOC);
                    foreach ($getOneRowWithCondition as $value) {
                        $this->insertRow($insertStmt, $row, $tableName, $sqlUnbinded);
                    }
                } else {
                    if ($tableName != $this->previousTable || $count == $this->rowCount) {
                        $checkIsInserted = $this->rowsChunk($this->previousTable, $this->batchs);
                        !$checkIsInserted && $this->splitChunkArray($this->previousTable, $sqlUnbinded, $totalFailed, $insertStmt);
                        $this->batchs = [];
                    }

                    array_push($this->batchs, $row);
                    $count++;
                }

                $tableName != $this->previousTable && $this->previousTable = $tableName;
                if ($this->rowNum >= 5) {
                    $this->end_time = microtime(true) - $this->time_pre;
                    $this->test_duration_rows = $this->end_time;
                    $this->processedRows++;
                    $this->displayProgress();
                } else {
                    // system('clear');
                    echo "waiting ...";
                }
            }

            if ($this->DEBUG_MODE) echo "\e[1;37;42m Inserted row in table $tableName\e[0m\n";

            $this->ErrorPlaces($tableName, $totalFailed);
            $this->checkIsDataSynced($tableName);
        } catch (Exception $e) {
            echo "\n\e[1;37;41mError syncing table $tableName: " . $e->getMessage() . "\e[0m\n";
            // table will be skipped
            return;
        }
    }

    /**
     * insert row in the target database
     *
     * @param  mixed $tableName
     * @param  mixed $batch
     * @param  mixed $columnList
     * @param  mixed $newColumns
     * @return void
     */
    private function insertBatch($tableName, $batch, $columnList, $newColumns)
    {
        $allColumns = $columnList . (!empty($newColumns) ? ", " . $this->getColumnList($newColumns) : "");
        $sql = $this->generateInsertStatement($tableName, $batch, $allColumns);

        try {
            $this->bdd2->exec($sql);
        } catch (PDOException $e) {
            echo "Error inserting batch in table $tableName: " . $e->getMessage() . "\n";
            $this->catchQueries[] = ["table" => $tableName, "query" => $sql, "exception" => $e->getMessage()];
        }
    }

    private function isIdenticalData($tableName, $sourceRow, $targetRow)
    {
        foreach ($sourceRow as $key => $value) {
            if (isset($targetRow[$key]) && $targetRow[$key] !== $value) {
                return false;
            }
        }
        return true;
    }

    /**
     * function to split the data array and try optimise the array to insert
     *
     * @param  mixed $tableName
     * @param  mixed $sqlUnbinded
     * @param  mixed $totalFailed
     * @param  mixed $insertStmt
     * @return void
     */
    private function splitChunkArray($tableName, $sqlUnbinded, $totalFailed, $insertStmt)
    {
        $half = count($this->batchs) / 2;
        $firstHalf = array_slice($this->batchs, 0, $half);
        $secondHalf = array_slice($this->batchs, $half);
        $firstResponse = $this->rowsChunk($tableName, $firstHalf);
        $secondResponse = $this->rowsChunk($tableName, $secondHalf);
        if (!$firstResponse) {
            foreach ($firstHalf as $value) {
                $totalFailed += $this->insertRow($insertStmt, $value, $tableName, $sqlUnbinded);
            }
        } else if (!$secondResponse) {
            foreach ($secondHalf as $value) {
                $totalFailed += $this->insertRow($insertStmt, $value, $tableName, $sqlUnbinded);
            }
        }
    }

    /**
     * function to give values for new columns in the target database table and put the default values from each column type
     *
     * @param  mixed $tableName
     * @return array
     */
    private function giveValuesForNewColumns($tableName)
    {
        $dbTwo = array_keys($this->db2Columns[$tableName]);
        $dbOne = array_keys($this->db1Columns[$tableName]);
        $arrayDiff = array_diff($dbTwo, $dbOne);
        $intersect_columns = array_intersect($arrayDiff, array_keys($this->db2Columns[$tableName]));
        // fach size dial l column kaykon mkhtalef 3la lakhor kayduplika dakchiii !!!!

        $newColumns = [];
        $newValues = [];
        if (!empty(array_values($intersect_columns))) {
            foreach (array_values($intersect_columns) as $value) {
                $val = $this->bdd2->query("SELECT DATA_TYPE FROM Information_Schema.Columns WHERE TABLE_NAME = '$tableName' AND COLUMN_NAME = '$value';")->fetchColumn();
                $IS_NULLABLE = $this->bdd2->query("SELECT IS_NULLABLE FROM Information_Schema.Columns WHERE TABLE_NAME = '$tableName' AND COLUMN_NAME = '$value';")->fetchColumn();
                $DefaultColumnValue = $this->bdd2->query("SELECT COLUMN_DEFAULT FROM Information_Schema.Columns WHERE TABLE_NAME = '$tableName' AND COLUMN_NAME = '$value';")->fetchColumn();

                if ($DefaultColumnValue == null && $IS_NULLABLE == "NO") {
                    array_push($newValues, $this->defaultValues[$val]);
                    array_push($newColumns, $value);
                    echo "\nhave no default value\n";
                } else {
                    echo "\nhave default value\n";
                }
            }
        }

        if (!empty($newValues)) {
            return ["cols" => $newColumns, "values" => $newValues];
        } else {
            return ["cols" => "", "values" => ""];
        }
    }

    /**
     * Display the progress of the synchronization
     *
     * @return void
     */
    private function displayProgress()
    {
        $estimatedTimeRemaining = $this->estimateRemainingTime();
        $progress = min(($this->processedRows / $this->totalRows) * 100, 100);

        echo sprintf(
            "\r⏱️  Progress: %.2f%% - Processed rows: %d/%d - Estimated time remaining: %s",
            $progress,
            $this->processedRows,
            $this->totalRows,
            $progress == 100 ? "Completed\n" : $this->formatTime($estimatedTimeRemaining)
        );
    }

    /**
     * Estimate the remaining time of the synchronization
     *
     * @return number
     */
    private function estimateRemainingTime()
    {
        $estimatedTimeRemaining = $this->totalRows * $this->test_duration_rows / $this->processedRows;
        return round(abs($estimatedTimeRemaining), 2);
    }

    /**
     * Format the time in seconds to a good readable format
     *
     * @param  mixed $seconds
     * @return string
     */
    private function formatTime($seconds)
    {
        if ($seconds < 60) {
            return round($seconds, 2) . ' seconds';
        } elseif ($seconds < 3600) {
            $minutes = floor($seconds / 60);
            $remainingSeconds = $seconds % 60;
            return sprintf('%d minutes and %.2f seconds', $minutes, $remainingSeconds);
        } else {
            $hours = floor($seconds / 3600);
            $remainingMinutes = ($seconds % 3600) / 60;
            return sprintf('%d hours and %.2f minutes', $hours, $remainingMinutes);
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
        echo "\nre_sync Related Tables...\n";
        foreach ($this->relatedTables as $name) {
            $this->compareTable($name, $this->db1Columns[$name]);
        }
        echo "done✅";
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
     * Generates a comma-separated list of values
     *
     * @param  mixed $valuesToInsert
     * @return string
     */
    private function getValuesList($valuesToInsert)
    {
        // !empty($valuesToInsert) && var_dump($valuesToInsert);
        // !empty($valuesToInsert) && die;
        return implode(', ', array_map(function ($col) {
            return "'$col'";
        }, $valuesToInsert));
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
     * Generates an insert statement for a given table and data
     *
     * @param  mixed $tableName
     * @param  mixed $data
     * @param  mixed $columnList
     * @return string
     */
    private function generateInsertStatement($tableName, $data, $columnList)
    {
        try {
            $values = [];
            $idxs = [];
            foreach ($data as $key => $row) {
                $rowValues = array_map(function ($value) {
                    return "'" . addslashes($value) . "'";
                }, $row);

                $values[] = '(' . implode(', ', $rowValues) . ')';
            }

            $valuesString = implode(",\n", $values);
            $sql = "INSERT INTO $tableName ($columnList) VALUES $valuesString";
            return $sql;
        } catch (Exception $e) {
            var_dump($e->getMessage());
            die;
        }
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

            // Check if the row already exists in the target database
            $existingRow = $this->getExistingRow($tableName, $valuesToInsert);

            if ($existingRow) {
                // Update the existing row with the new data
                $updatedRow = array_replace($existingRow, $valuesToInsert);
                $isUpdated = $this->updateRow($tableName, $updatedRow, $valuesToInsert);
                return $isUpdated ? 0 : 1;
            } else {
                // Insert the new row
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
                return 0;
            }
        } catch (PDOException $e) {
            echo "Error inserting row in table $tableName: " . $e->getMessage() . "\n";
            return 1;
        }
    }

    /**
     * getExistingRow
     *
     * @param  mixed $tableName
     * @param  mixed $row
     */
    private function getExistingRow($tableName, $row)
    {
        $whereClause = [];
        foreach ($row as $column => $value) {
            $whereClause[] = "`$column` = '" . addslashes($value) . "'";
        }
        $whereClause = implode(' AND ', $whereClause);

        $selectQuery = $this->bdd2->prepare("SELECT * FROM `$this->dbname2`.`$tableName` WHERE $whereClause LIMIT 1");
        $selectQuery->execute();
        return $selectQuery->fetch(PDO::FETCH_ASSOC);
    }

    private function updateRow($tableName, $updatedRow, $row)
    {
        $updateQuery = "UPDATE `$this->dbname2`.`$tableName` SET ";
        $updateValues = [];
        foreach ($updatedRow as $column => $value) {
            $updateValues[] = "`$column` = '" . addslashes($value) . "'";
        }
        $updateQuery .= implode(', ', $updateValues);
        $updateQuery .= " WHERE " . implode(' AND ', array_map(function ($col) use ($row) {
            return "`$col` = '" . addslashes($row[$col]) . "'";
        }, array_keys($row)));

        try {
            $this->bdd2->exec($updateQuery);
            return true;
        } catch (PDOException $e) {
            echo "Error updating row in table $tableName: " . $e->getMessage() . "\n";
            return false;
        }
    }

    /**
     * insert rows 1000 by 1000 to make time of execution faster
     * @param string $tableName
     * @param array $arrayOfData
     * @return bool
     */
    private function rowsChunk($tableName, $arrayOfData)
    {
        try {
            $columnList = $this->getColumnList($this->getColumnsToInsert($tableName));
            $chunkSize = 1000;
            $numChunks = ceil(count($arrayOfData) / $chunkSize);

            for ($i = 0; $i < $numChunks; $i++) {
                $chunk = array_slice($arrayOfData, $i * $chunkSize, $chunkSize);
                $sql = $this->generateUpsertStatement($tableName, $chunk, $columnList);
                $insertStmt = $this->bdd2->prepare($sql);
                $checkIsInserted = $insertStmt->execute();

                if (!$checkIsInserted) {
                    // if($tableName == ""){
                    $this->catchQueries[] = ["table" => $tableName, "query" => $insertStmt->queryString, "exception" => $insertStmt->errorInfo()[2]];
                    return false;
                    // }
                }
            }
        } catch (Exception $e) {
            echo "Error : " . $e->getMessage() . "\n";
            return false;
        }
        return true;
    }

    private function generateUpsertStatement($tableName, $data, $columnList)
    {
        $values = [];
        $notNullColumns = [];
        $notNullColumns = $this->bdd2->query("SELECT COLUMN_NAME FROM Information_Schema.Columns WHERE TABLE_SCHEMA = '$this->dbname2' AND TABLE_NAME = '$tableName' AND IS_NULLABLE = 'YES';")->fetchAll(PDO::FETCH_COLUMN);
        $cols = [];
        foreach ($data as $row) {
            $cols = [];
            $rowValues = [];
            $cols = array_keys($row);

            array_walk($row, function ($value, $idx) use (&$rowValues, $notNullColumns) {
                if (in_array($idx, $notNullColumns) && $value == '') {
                    $rowValues[] = 'NULL';
                } else {
                    $rowValues[] = "'" . addslashes($value) . "'";
                }
            });

            $values[] = '(' . implode(', ', $rowValues) . ')';
        }

        $valuesString = implode(",\n", $values);
        $updateString = implode(", ", array_map(function ($col) {
            return "$col = VALUES($col)";
        }, explode(", ", $columnList)));

        if ($tableName == "ps_employee_session" || $tableName == "ps_customer_session") {
            $columnList = $this->getColumnList($cols);
        }
        $sql = "INSERT INTO $tableName ($columnList) VALUES $valuesString
        ON DUPLICATE KEY UPDATE $updateString";

        return $sql;
    }

    private function checkIsDataSynced($tableName)
    {
        $stmt = $this->bdd1->prepare("SELECT COUNT(*) FROM `$tableName`");
        $stmt->execute();
        $count1 = $stmt->fetchColumn();

        $stmt = $this->bdd2->prepare("SELECT COUNT(*) FROM `$tableName`");
        $stmt->execute();
        $count2 = $stmt->fetchColumn();

        array_push($this->tableStats, ["table" => $tableName, "count1" => $count1, "count2" => $count2]);
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
        if ($this->DEBUG_MODE) {
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
        } else {
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
     * function to export file for table stats 
     *
     * @return void
     */
    private function handleTableStats()
    {
        if (!empty($this->tableStats)) {
            $output = '';
            foreach ($this->tableStats as $stats) {
                $output .= "Table: " . $stats["table"] . "\n";
                $output .= "table 1: " . $stats["count1"] . ";\n\n";
                $output .= "table 2: " . ($stats["count2"] ?? '') . ";\n\n";
                $output .= "status ". $stats["count2"] == $stats["count1"] ? "✅" : "❌" .";\n\n";
                $output .= "\n_____________________________________________________________________________________________________\n";
            }
            file_put_contents("stats.txt", $output);
            echo "table stats have been saved to stats.txt\n";
        } else {
            echo "No stats to save.\n";
        }
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

$syncer = new DatabaseSyncer('allo_7', 'verify_8_2', false, false, false);
$syncer->syncDatabases();
