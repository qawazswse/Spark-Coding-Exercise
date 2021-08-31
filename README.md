# Spark-Coding-Exercise

Simple Spark project runs in local environment.

##Description

* Contains two csv files as local data to analyze.
* Takes 3 parameters:
  
    * The first parameter is a number from 0-2 indicates which analyze to run.
        ```
        analyze 0: calculate total receipt price for each state in descending order
        analyze 1: calculate average discount percentage for each category in descending order
        analyze 2: calculate total receipt price for each user in descending order
        ```
    * The second parameter is a number indicates last x month
        ```
        Since the data is now the newest, this parameter better larger than 12
        ```
    * The third parameter is a String indicates the type of the output file
        ```
        There are 3 type of output files supported: csv, json, parquet
        ```
    * when the program finish running, the output file will be generated in the "output" folder
    
## Getting Started  
###Dependencies:
* Windows 10
* JDK 11

### Executing program
* Using terminal, cd to "Spark Coding Exercise/target/" 
* Run "java -jar spark_coding_exercise-1.0-SNAPSHOT.jar [parameters]"
* Get the output file in "Spark Coding Exercise/output/" folder
## Authors
Contributors names and contact info
* @qawazswse
## Version History
* 0.4
  * finished the main of the program
  * packed the program into a jar package in target folder
  * finished the readme.md file
  * *** The jar file is too large (over 100Mb), currently trying solutions...
* 0.3
    * achieved the 3rd analyze
    * achieved output-to-file goal for 3 analyzes
    * added result_objects folder to hold result record classes
* 0.2
    * fixed a possible bug for analyze one
    * achieved the second analyze
    * separated csv to rdd functions from service package to csv_to_rdd package
* 0.1
    * Achieved first analyze
    * added opencsv dependency
    * added lombok dependency
    * built class for both csv file data
    * solved comma problem in the csv file... in a way
