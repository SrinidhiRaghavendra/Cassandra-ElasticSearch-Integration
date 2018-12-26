#!/bin/bash
echo $PWD
java -cp $PWD/target/c-es-1.0-SNAPSHOT.jar:lib/*:conf/* CassandraToES.InitialData "$@"
java -cp $PWD/target/c-es-1.0-SNAPSHOT.jar:lib/*:conf/* CassandraToES.IncrementalData "$@"
