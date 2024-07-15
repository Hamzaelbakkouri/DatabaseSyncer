<?php

// Example of usage : php sync-db-ps17-to-ps17.php
// Ce script sert à transféré les données d'une BDD PS à la même version de PS

// MODE DEBUG ACTIVE VARIABLE
$DEBUG_MODE = false;
// MODE DEBUG ACTIVE VARIABLE



$dbname1 = 'test_old';
$dbname2 = 'test_new';

// make $handleProblemInstantinously if you want to fix queries every time
$handleProblemInstantinously = false;

error_reporting(E_ALL);
ini_set('display_errors', 1);

$results = [];
$tables = [];
$exeptions = [];
$tablesIgnore = [
    "ps_configuration",
    "ps_shop",
    "ps_shop_url"
];
$errorPlaces = [];
$catchQueries = [];
$totalFailed = 0;
try {
    // Connect to both databases
    $bdd1 = new PDO('mysql:host=localhost;dbname=' . $dbname1 . ';charset=utf8', "root", "");
    $bdd2 = new PDO('mysql:host=localhost;dbname=' . $dbname2 . ';charset=utf8', "root", "");

    // Fetch column information from both databases
    $columnsQuery1 = $bdd1->query("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" . $dbname1 . "';");
    $columnsQuery2 = $bdd2->query("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" . $dbname2 . "';");

    $columns1 = $columnsQuery1->fetchAll(PDO::FETCH_ASSOC);
    $columns2 = $columnsQuery2->fetchAll(PDO::FETCH_ASSOC);

    // Convert results to associative arrays for easier comparison
    $db1Columns = [];
    foreach ($columns1 as $column) {
        $db1Columns[$column['TABLE_NAME']][$column['COLUMN_NAME']] = $column['COLUMN_TYPE'];
    }

    $db2Columns = [];
    foreach ($columns2 as $column) {
        $db2Columns[$column['TABLE_NAME']][$column['COLUMN_NAME']] = $column['COLUMN_TYPE'];
    }

    $identical = true;
    $schemaChanged = false;

    // foreach for old db tables
    foreach ($db1Columns as $tableName => $columns) {
        $totalFailed = 0;
        // check is table not the ignored tables
        if (!in_array($tableName, $tablesIgnore)) {

            // remove all the data in the table
            $truncateQuery = "TRUNCATE TABLE `$dbname2`.`$tableName`";
            $bdd2->query($truncateQuery);

            $sql2 = $bdd2->query("SHOW COLUMNS FROM `" . $tableName . "`");
            if (!$sql2) {
                echo "____________________________________________________________________";
                var_dump($bdd2->errorInfo());
                echo "____________________________________________________________________";
                die;
            } else {
                $columnsToInsert = $sql2->fetchAll(PDO::FETCH_COLUMN);

                // split column for query form
                $columnList = implode(', ', array_map(function ($col) {
                    return "`$col`";
                }, $columnsToInsert));

                // binding signs
                $placeholders = implode(', ', array_fill(0, count($columnsToInsert), '?'));

                $selectQuery = $bdd1->query("SELECT $columnList FROM `$dbname1`.`$tableName`");

                // put columns and binding signs into query
                $sql = "INSERT INTO `$dbname2`.`$tableName` ($columnList) VALUES ($placeholders)";
                $insertStmt = $bdd2->prepare($sql);

                while ($row = $selectQuery->fetch(PDO::FETCH_ASSOC)) {
                    // handle date problem "0000-00-00" and "0000-00-00 00:00:00"
                    foreach ($row as $key => $value) {
                        if ($value == "0000-00-00 00:00:00" || $value == "0000-00-00") {
                            if (strlen($value) == 19) {
                                $row[$key] = '1970-01-01 00:00:00';
                            } else {
                                $row[$key] = '1970-01-01';
                            }
                        }
                    }


                    // get the data to put into the fake query
                    $rowValues = implode(", ", array_map(function ($col) {
                        return $col;
                    }, $row));


                    // fake query to console show
                    $sqlUnbinded = "INSERT INTO `$dbname2`.`$tableName` ($columnList) VALUES ($rowValues)";


                    try {

                        // execute the real query and sent data binded
                        $isInserted = $insertStmt->execute(array_values($row));

                        if (!$isInserted) {
                            // mode (handle Problem Instantinously) => IN
                            if ($handleProblemInstantinously) {
                                $check = 0;
                                // check the collect or fix
                                echo "\n" . $sqlUnbinded . ";\n";
                                echo "\e[1;37;43m query show problems, 1-fix ,else to collect : \e[0m\n";
                                $check = trim(fgets(fopen("php://stdin", "r")));

                                if (ctype_digit(intval($check)) && intval($check) == 1) {
                                    echo "\e[1;37;43m Enter Fixed query : \e[0m\n";
                                    $fin = fopen("php://stdin", "r");
                                    $line = fgets($fin);
                                    $secondChance = $bdd2->prepare($line);
                                    $secondChance->execute();
                                } else {
                                    $totalFailed++;
                                    array_push($catchQueries, ["table" => $tableName, "query" => $sqlUnbinded]);
                                }
                                // mode (handle Problem Instantinously) => OUT
                            } else {
                                $totalFailed++;
                                $sqlExeption = $insertStmt->errorInfo();
                                array_push($catchQueries, ["table" => $tableName, "query" => $sqlUnbinded, "exeption" => $sqlExeption[2]]);
                            }
                        }
                    } catch (PDOException $e) {
                        echo "Error inserting row in table $tableName: " . $e->getMessage() . "\n";
                    }
                }

                if ($DEBUG_MODE) echo "\e[1;37;42m Inserted row in table $tableName\e[0m\n";
            }


            // foreach for old db tables
            foreach ($columns as $columnName => $columnType) {
                if (!isset($db2Columns[$tableName][$columnName])) {
                    // $results[] = "Column $columnName added to table $tableName in Database 2 and data copied";
                    $identical = false;
                } elseif ($db2Columns[$tableName][$columnName] != $columnType) {
                    $exeptions = [$db2Columns[$tableName][$columnName] => "table => " . $tableName . " column => " . $columnName . " type problem"];
                }
                $identical = false;
            }
        } else {
            echo "\e[1;37;41m" . $tableName . " not inserted because its depends directly on the project \e[0m\n";
        }
        $count = $bdd1->query("SELECT COUNT(*) FROM `" . $tableName . "`");
        $count = $count->fetch();
        array_push($errorPlaces, ["table" => $tableName, "total" => $count[0], "error" => $totalFailed]);
    }


    // Start of schema creation
    if ($identical) {
        $tables[] = "tables are identical";
        $results[] = "columns are identical";
    }

    // End of schema creation
    foreach ($tables as $line) {
        echo $line . "\n";
    }
    foreach ($results as $line) {
        echo $line . "\n";
    }
    if (isset($exeptions)) {
        foreach ($exeptions as $line => $value) {
            echo "\n\e[1;37;41m" . "Column /" . $line . "/ : " . $value . "\e[0m\n";
        }
    }

    echo "\n______________________________________________________________________________________________________________________________________________________________________\n\n";
    if (isset($errorPlaces)) {
        foreach ($errorPlaces as $key => $value) {
            $succes = $value["total"] - $value["error"];
            if ($value["error"] > 0) {
                echo  "❌ " . $value["table"] . " : (" . $value["error"] . " erreur/" . $value["total"] . " Total)\n";
            }
        }
    }
    echo "\n______________________________________________________________________________________________________________________________________________________________________\n";

    echo "\n1- create file for catched queries ,else to skip: \n\n";
    $v = trim(fgets(fopen("php://stdin", "r")));
    if ($v == 1) {
        if (!empty($catchQueries)) {
            $output = '';
            foreach ($catchQueries as $query) {
                $output .= "Table: " . $query["table"] . "\n";
                $output .= "Query: " . $query["query"] . ";\n\n";
                $output .= "Exeption: " . $query["exeption"] . ";\n\n";
                $output .= "\n_____________________________________________________________________________________________________\n";
            }
            file_put_contents("queriesCached.txt", $output);
            echo "Queries have been saved to queriesCached.txt\n";
        } else {
            echo "No queries to save.\n";
        }
    }
    echo "program out";
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
# ps_product_shop : 16/19 (16 succès/ 19 Total)
# ps_product_shop : 16/19 (3 erreur/ 19 Total)

// 0000-00-00 00:00:00
// 0000-00-00