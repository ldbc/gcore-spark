# G-CORE interpreter

## To build and run
The project will build successfully under Java 8. Spark 2.2.0 is needed to run
the application. Spark can be downloaded from
https://spark.apache.org/downloads.html.

To sumbit on Spark, the project needs to be compiled as an uber-jar. The
-DskipTests flag can be used with the mvn package command to avoid running the
tests when packaging the uber-jar.

The Spoofax parser uses Guice as a dependency injection framework. The Guice 4.0
jar needs to be passed separately to the driver as a
```spark.driver.extraClassPath``` property, otherwise the driver is not able to
find it.

```bash
mvn package -DskipTests
spark-submit \
    --class GcoreRunner \
    --master local[2] \
    --conf "spark.driver.extraClassPath=/path_to/guice-4.0.jar" \
    target/gcore-interpreter-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## To run tests
```bash
mvn test
```
