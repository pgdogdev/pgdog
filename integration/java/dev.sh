#!/bin/bash
CLASS_PATH="$PWD:$PWD/postgres.jar"
javac pgdog.java
java -cp ${CLASS_PATH} -ea Pgdog
