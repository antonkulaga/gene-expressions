#!/bin/bash 
echo $($SPARK_HOME/bin/spark-submit --class org.denigma.genes.GeneExpressions $HOME/denigma/gene-expressions/target/scala-2.10/gene-expressions.jar)
exit 0