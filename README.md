# UniBench 2.0: A benchmark for multi-model databases
The [UniBench project](https://www.helsinki.fi/en/researchgroups/unified-database-management-systems-udbms/unibench-towards-benchmarking-multi-model-dbms) aims to develop a generic benchmark for a holistic evaluation of multi-model database systems (MMDS), which are able to support multiple data models such as documents, graph, and key-value models in a single back-end. UniBench consists of a set of mixed data models that mimics a social commerce application, which covers data models including JSON, XML, key-value, tabular,  graph, and RDF. The UniBench workload consists of a set of complex read-only queries and read-write transactions that involve at least two data models.

Please access our [Big Data 2020 tutorial](https://www.helsinki.fi/en/researchgroups/unified-database-management-systems-udbms/ieee-big-data-2020-tutorial), [DAPD 2019 journal paper](http://link.springer.com/article/10.1007/s10619-019-07279-6), and [TPCTC 2018 paper](https://www.cs.helsinki.fi/u/jilu/documents/UniBench.pdf) to find more details:

```
Zhang, Chao, Jiaheng Lu. "Big Data System Benchmarking: State of the Art, Current Practices, and Open Challenges." In IEEE BIG DATA 2020 TUTORIAL, 2020.

Zhang, Chao, Jiaheng Lu. "Holistic Evaluation in Multi-Model Databases Benchmarking." In Distributed and Parallel Databases, 2019.

Zhang, Chao, et al. "UniBench: A benchmark for multi-model database management systems." TPCTC. Springer, Cham, 2018.
```

## Query Implementations
Essentially, any MMDB can be implemented in UniBench either with or without data transformation. Since there is no query language standard, one can ﬁnd all the query deﬁnitions in ArangoDB AQL, OrientDB SQL and AgensGraph SQL/Cypher, Spark SQL (partially) as follows:

| [Query](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/MMDB.java)  | 01 | 02 | 03 | 04 | 05 | 06 | 07| 08 | 09| 10 |
| -------------- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ArangoDB (AQL) | [01](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Arango.java) | [02](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Arango.java) | [03](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Arango.java) | [04](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Arango.java) | [05](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Arango.java) | [06](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Arango.java) | [07](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Arango.java) | [08](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Arango.java) | [09](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Arango.java) | [10](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Arango.java) |
| OrientDB (SQL) | [01](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/OrientDB.java)  | [02](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/OrientDB.java)  | [03](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/OrientDB.java)  | [04](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/OrientDB.java)  | [05](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/OrientDB.java)  | [06](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/OrientDB.java)  | [07](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/OrientDB.java)  | [08](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/OrientDB.java)  | [09](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/OrientDB.java)  | [10](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/OrientDB.java)  |
| AgensGraph (Cypher/SQL) | [01](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/AgensGraph.java)  | [02](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/AgensGraph.java)  | [03](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/AgensGraph.java)  | [04](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/AgensGraph.java)  | [05](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/AgensGraph.java)  | [06](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/AgensGraph.java)  | [07](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/AgensGraph.java)  | [08](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/AgensGraph.java)  | [09](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/AgensGraph.java)  | [10](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/AgensGraph.java)  |
| Spark (SQL)     | [01](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Spark.java)  | 02 | 03 | 04 | [05](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Spark.java) | 06 | 07 | [08](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/Spark.java)  | 09  | 10  |

## Running

Please follow the instructions in [wiki](https://github.com/Rucchao/UniBench_new/wiki) to run the benchmark.
