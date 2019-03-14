Packaged jar can have throw errors. Use the below command:
zip -d /Users/sunitakoppar/ucr/projects/cafs/target/cafs-1.0-jar-with-dependencies.jar  META-INF/LICENSE

Trigger Map - reduce to Index the input data. 
hadoop jar /Users/sunitakoppar/ucr/projects/cafs/target/cafs-1.0-jar-with-dependencies.jar partb.HadoopIndexer /tweets /hadoopScoredIndex

hadoop fs -get /hadoopScoredIndex 
Place the file in root of this repository

java -cp cafs-1.0-jar-with-dependencies.jar partb.SearchScoredHadoopIndex hadoopScoredIndex/part-00000 Adbowl 

