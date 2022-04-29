# AirDNA_Program
AirDNA IngestJob and ExportJob


1. Created a Maven Project in IntelliJ

2. Import SDK 1.8 java version

3. Import scala-sdk-2.12.10 to IntelliJ project

4. Import the files to folder
	- IngestJob.scala & ExportJob.scala to /src/main/scala 
	- amenities.txt & properties.json to /src/main/resources/assignment

5. Edit the pom.xml file and include the spark dependency

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.12</artifactId>
            <version>3.1.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>3.1.2</version>
        </dependency>

6. Build the Maven project to create a AirDNA_Program-1.0-SNAPSHOT.jar file

7. Create a new Application Configeration
	- IngestJob
	- ExportJob

8. Run IngestJob application
	- Ingest the properties.json file and create parquet file to /output/assignment/properties
	- Ingest the amenities.txt file and create a parquet file to /output/assignment/amenties 

9. Run ExportJob application
	- Read parquet and export it to properties_amenties & properties_amenties_summary_report
