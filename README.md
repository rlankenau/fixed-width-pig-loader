fixed-width-pig-loader
======================

Pig LoadFunc for fixed-width data


Building
--------

$mvn package

Running
-------

Sample pig script for use with this loader:

	register 'maprfs:///user/rlankenau/FixedWidthLoader-1.0-SNAPSHOT.jar';
	
	A = load 'maprfs:///user/rlankenau/testfile' using com.mapr.util.FixedWidthLoader('-10','11-15','16-','-');
	store A into 'maprfs:///user/rlankenau/outputfile' using PigStorage(',');
