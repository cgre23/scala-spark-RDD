# scala-spark-RDD
Using Spark RDD to aggregate transactions from three files and calculate net profit by country:
currency.csv -> currency rates 
transactions.csv -> all the transactions made
customers.csv -> Customer information and country

*	by Christian Grech (interviewing for Machine Learning Engineer)
*	Date: Fri 1st November 2019
*
*	Project name: test
*	Main Scala file: 	test/src/main/scala/bigdata/rddchallenge.scala
*	Input CSV files:	target/src/main/resources/datasets
*	Output CSV files: 	target/output_destination      !!!! These are the results !!!!!!!
*	Jar files:		test/target/test-1.0-SNAPSHOT-jar-with-dependency.jar	created using 'mvn package' from local terminal
*	Shell file		test/shell-script		!!! runs successfully !!!
*	The dependencies to produce fat jar are included in pom.xml. The fat jar is already generated. 
*	If you want to regenerate the fat jar please do not use mvn clean as this will delete input csv files in target folder.
*	
*
*	Java JDK version: 1.8.0_231
*	Scala: 2.11
*	Spark: 2.11 version 2.2.0 as listed in pom.xml
*	Intellij IDEA 2019.2.4
*
*
