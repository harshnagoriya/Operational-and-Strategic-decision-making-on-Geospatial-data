
# Operational and Strategic decision making on Geospatial-data using Spark

## Description

The collection and processing of Spatio-temporal data have increased dramatically in recent years. Processing them and producing meaningful information from them remains a major challenge for businesses and tech organizations. The technique of extracting a data subset from a map layer by dealing closely with the map data is known as a spatial query. Data is stored in attribute tables and feature/spatial tables inside a spatial database. The goal of this project is to execute various geographical queries for a big peer-to-peer taxi cab company’s operational (day-to-day) and strategic (long-term) decisions.

### More details in the project report

## Significance

The data of a town or city’s geographical boundaries, as well as a set of points P indicating clients who want taxi cab service, were processed in this project. The Apache Spark framework was used to overcome the problem of large-scale geographic data computation. As a result, Apache Spark proved to be an effective choice for achieving high performance. We were able to properly analyze geospatial data primarily with the use of SparkSQL APIs on Scala.
We designed an algorithm that extracts essential information from a dataset using Apache Spark and Scala, which can then be used to make operational and strategic decisions. There were also a few specific spatial queries built. Our developed system gives the client statistically significant geographic areas to help them plan their business and better serve their customers.


## Compilation Steps

1. Go to the project root folder
2. Run ```sbt assembly```. You may need to install sbt in order to run this command.
3. Find the packaged jar in "./target/scala-2.11/CSE512-Project-Phase2-Template-assembly-0.1.0.jar"
4. Submit the jar to Spark using the Spark command "./bin/spark-submit"

#### Contributors

- [Harsh Nagoriya](https://www.linkedin.com/in/harshnagoriya/)
- [Krushali Shah](https://www.linkedin.com/in/krushali-shah/)
- [Savan Doshi](https://www.linkedin.com/in/savand/)

