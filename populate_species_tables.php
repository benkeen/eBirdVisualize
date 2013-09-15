<?php

ini_set('memory_limit', '512M');
set_time_limit(100000000);


// this converts notices into errors so we can catch them
set_error_handler('exceptions_error_handler');
function exceptions_error_handler($severity, $message, $filename, $lineno) {
	if (error_reporting() == 0) {
		return;
	}
	if (error_reporting() & $severity) {
		throw new ErrorException($message, 0, $severity, $filename, $lineno);
	}
}

//mysql_connect("localhost", "root", "root");
//mysql_select_db("eBirdData");


// some globals
$uniqueSpecies = array();
$uniqueSpeciesString = "";
$sourceFile = "../eBirdDataSource/eBirdData_May-2013.txt";
//$sourceFile = "../eBirdDataSource/tenmillion.txt";
//$sourceFile = "../eBirdDataSource/million.txt";

$time_start = microtime(true);


parseSourceFile();
generateSpeciesTables();



function parseSourceFile() {

	global $sourceFile;

	$file = @fopen($sourceFile, "r");

	$prevLine = "";
	$lastLineHadProblem = false;
	$lineNum = 0;
	while (($buffer = fgets($file, 4096)) !== false) {
		$line = trim($buffer);
		if (empty($line)) {
			continue;
		}
		$cols = explode("\t", $line);
		$numCols = count($cols);

		// if the last line had a problem, see if we can make an educated guess at repairing it
		if ($lastLineHadProblem) {
			$lastLineNum = $lineNum - 1;
			$lastLineCols = explode("\t", $prevLine);
			$lastLineNumCols = count($lastLineCols);

			$joinCount = $lastLineNumCols + $numCols;
			if ($joinCount == 40 || $joinCount == 41) {
				$joinedLineCols = explode("\t", $prevLine . $line);

				processRecord($joinedLineCols);
			}
			else
			{
				if ($joinCount > 41) {
					echo "*** Something was wrong with line $lastLineNum. It only had $lastLineNumCols columns.\n";
					echo "*** the new line has $numCols columns so combining them doesn't solve it. ***\n";
					echo $prevLine . "\n";
					echo $line . "\n_______________________________\n";

				// if the joined count was still less than 40, just continue so we keep tacking on rows. Maybe the
				// new row will complete it
				} else {
					$prevLine = $prevLine . $line;
					continue;
				}
			}
			echo "\n";

			$lastLineHadProblem = false;
			continue;
		}

		if ($numCols == 40 || $numCols == 41) {
			$lastLineHadProblem = false;
			processRecord($cols);

			// here there's something bad with the row. Make a note of it.
		} else {
			$lastLineHadProblem = true;
			$prevLine = $line;
		}
		$lineNum++;
	}
}


function generateSpeciesTables() {
	global $uniqueSpecies;

	$cleanNames = array();
	foreach ($uniqueSpecies as $speciesName) {
		$speciesName = strtolower($speciesName);
		$cleanName = preg_replace("/[^a-z]/", "", $speciesName);
		$cleanNames[] = "sp_$cleanName";
	}


	if (arrayHasDuplicates($cleanNames)) {

	} else {
		echo count($cleanNames);

		echo "___";

		foreach ($cleanNames as $tableName) {
			echo generateTableSQL($tableName);
		}
	}
}



$time_end = microtime(true);

//echo "Num species: " . count($uniqueSpecies) . "\n";

$execution_time = ($time_end - $time_start) / 60;

echo "_____________________\n";
echo 'Total Execution Time: '.$execution_time.' mins';


function processRecord($cols) {
	global $uniqueSpecies, $uniqueSpeciesString;

	// prep each column for DB insertion
//	$cleanCols = array();
//	foreach ($cols as $col) {
//		$cleanCols[] = mysql_real_escape_string(trim($col));
//	}

	$commonName = $cols[3];
	if (strpos($uniqueSpeciesString, "$commonName|") === false) {
		$uniqueSpeciesString .= "$commonName|";
		$uniqueSpecies[] = $commonName;
	}
}


function arrayHasDuplicates($array) {
	return count($array) !== count(array_unique($array));
}

function getDuplicates($array) {
	return array_unique(array_diff_assoc($array, array_unique($array)));
}

function generateTableSQL($tableName) {
	$sql =<<< END
CREATE TABLE $tableName (
  `GUID` varchar(50) NOT NULL,
  `TAXONOMIC_ORDER` float NOT NULL,
  `CATEGORY` varchar(7) NOT NULL,
  `COMMON_NAME` TEXT NOT NULL,
  `SUBSPECIES_COMMON_NAME` TEXT NOT NULL,
  `OBSERVATION_COUNT` TEXT NOT NULL,
  `AGE_SEX` varchar(100) NOT NULL,
  `COUNTRY_CODE` varchar(2) NOT NULL,
  `STATE_CODE` varchar(5) NOT NULL,
  `COUNTY_CODE` varchar(10) NOT NULL,
  `IBA_CODE` varchar(10) NOT NULL,
  `LOCALITY_ID` varchar(10) NOT NULL,
  `LOCALITY_TYPE` varchar(1) NOT NULL,
  `LATITUDE` float NOT NULL,
  `LONGITUDE` float NOT NULL,
  `OBSERVATION_DATE` date NOT NULL,
  `TIME_OBSERVATIONS_STARTED` varchar(10) NOT NULL,
  `DURATION_MINUTES` varchar(5) NOT NULL,
  `EFFORT_DISTANCE_KM` varchar(5) NOT NULL,
  `EFFORT_AREA_HA` varchar(1) NOT NULL,
  `NUMBER_OBSERVERS` varchar(1) NOT NULL,
  `ALL_SPECIES_REPORTED` varchar(1) NOT NULL,
  `APPROVED` varchar(1) NOT NULL,
  `REVIEWED` varchar(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

END;

	return $sql;
}


/*
 * CREATE TABLE $tableName (
  `GUID` varchar(50) NOT NULL,
  `TAXONOMIC_ORDER` float NOT NULL,
  `CATEGORY` varchar(7) NOT NULL,
  `SCIENTIFIC_NAME` varchar(100) NOT NULL,
  `COMMON_NAME` varchar(100) NOT NULL,
  `SUBSPECIES_COMMON_NAME` varchar(50) NOT NULL,
  `SUBSPECIES_SCIENTIFIC_NAME` varchar(30) NOT NULL,
  `OBSERVATION_COUNT` varchar(5) NOT NULL,
  `BREEDING_BIRD_ATLAS_CODE` varchar(100) NOT NULL,
  `AGE_SEX` varchar(100) NOT NULL,
  `COUNTRY` varchar(20) NOT NULL,
  `COUNTRY_CODE` varchar(2) NOT NULL,
  `STATE` varchar(10) NOT NULL,
  `STATE_CODE` varchar(5) NOT NULL,
  `COUNTY` varchar(10) NOT NULL,
  `COUNTY_CODE` varchar(10) NOT NULL,
  `IBA_CODE` varchar(10) NOT NULL,
  `LOCALITY` varchar(50) NOT NULL,
  `LOCALITY_ID` varchar(10) NOT NULL,
  `LOCALITY_TYPE` varchar(1) NOT NULL,
  `LATITUDE` float NOT NULL,
  `LONGITUDE` float NOT NULL,
  `OBSERVATION_DATE` varchar(10) NOT NULL,
  `TIME_OBSERVATIONS_STARTED` varchar(10) NOT NULL,
  `TRIP_COMMENTS` varchar(250) NOT NULL,
  `SPECIES_COMMENTS` varchar(10) NOT NULL,
  `OBSERVER_ID` varchar(10) NOT NULL,
  `FIRST_NAME` varchar(20) NOT NULL,
  `LAST_NAME` varchar(20) NOT NULL,
  `SAMPLING_EVENT_IDENTIFIER` varchar(10) NOT NULL,
  `PROTOCOL_TYPE` varchar(30) NOT NULL,
  `PROJECT_CODE` varchar(15) NOT NULL,
  `DURATION_MINUTES` varchar(5) NOT NULL,
  `EFFORT_DISTANCE_KM` varchar(5) NOT NULL,
  `EFFORT_AREA_HA` varchar(1) NOT NULL,
  `NUMBER_OBSERVERS` varchar(1) NOT NULL,
  `ALL_SPECIES_REPORTED` varchar(1) NOT NULL,
  `GROUP_IDENTIFIER` varchar(1) NOT NULL,
  `APPROVED` varchar(1) NOT NULL,
  `REVIEWED` varchar(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

 */