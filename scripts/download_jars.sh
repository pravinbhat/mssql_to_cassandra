#!/bin/bash
# Download required JDBC drivers for Spark

set -e

JARS_DIR="jars"
mkdir -p "$JARS_DIR"

echo "Downloading MSSQL JDBC driver..."
MSSQL_VERSION="12.8.1.jre11"
MSSQL_JAR="mssql-jdbc-${MSSQL_VERSION}.jar"
MSSQL_URL="https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/${MSSQL_VERSION}/${MSSQL_JAR}"

if [ ! -f "$JARS_DIR/$MSSQL_JAR" ]; then
    curl -L -o "$JARS_DIR/$MSSQL_JAR" "$MSSQL_URL"
    echo "Downloaded $MSSQL_JAR"
else
    echo "$MSSQL_JAR already exists"
fi

echo "Downloading Cassandra Spark Connector..."
CASSANDRA_VERSION="3.5.0"
CASSANDRA_JAR="spark-cassandra-connector-assembly_2.12-${CASSANDRA_VERSION}.jar"
CASSANDRA_URL="https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/${CASSANDRA_VERSION}/${CASSANDRA_JAR}"

if [ ! -f "$JARS_DIR/$CASSANDRA_JAR" ]; then
    curl -L -o "$JARS_DIR/$CASSANDRA_JAR" "$CASSANDRA_URL"
    echo "Downloaded $CASSANDRA_JAR"
else
    echo "$CASSANDRA_JAR already exists"
fi

echo "All JARs downloaded successfully!"
echo "JARs location: $JARS_DIR/"
ls -lh "$JARS_DIR/"
