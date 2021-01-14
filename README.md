# UniBench 2.0: A benchmark for multi-model databases
The [UniBench project](https://www.helsinki.fi/en/researchgroups/unified-database-management-systems-udbms/unibench-towards-benchmarking-multi-model-dbms) aims to develop a generic benchmark for a holistic evaluation of multi-model database systems (MMDS), which are able to support multiple data models such as documents, graph, and key-value models in a single back-end. UniBench consists of a set of mixed data models that mimics a social commerce application, which covers data models including JSON, XML, key-value, tabular,  graph, and RDF. The UniBench workload consists of a set of complex read-only queries and read-write transactions that involve at least two data models.

Please access our [Big Data 2020 tutorial](https://www.helsinki.fi/en/researchgroups/unified-database-management-systems-udbms/ieee-big-data-2020-tutorial), [DAPD 2019 journal paper](http://link.springer.com/article/10.1007/s10619-019-07279-6), and [TPCTC 2018 paper](https://www.cs.helsinki.fi/u/jilu/documents/UniBench.pdf) to find more details:

```
Zhang, Chao, Jiaheng Lu. "Big Data System Benchmarking: State of the Art, Current Practices, and Open Challenges." In IEEE BIG DATA 2020 TUTORIAL, 2020.

Zhang, Chao, Jiaheng Lu. "Holistic Evaluation in Multi-Model Databases Benchmarking." In Distributed and Parallel Databases, 2019.

Zhang, Chao, et al. "UniBench: A benchmark for multi-model database management systems." TPCTC. Springer, Cham, 2018.
```

## Query Implementations

| [Query](https://github.com/HY-UDBMS/UniBench/blob/master/Unibench/src/MMDB.java)  | 01 | 02 | 03 | 04 | 05 | 06 | 07| 08 | 09| 10 |
| -------------- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ArangoDB (AQL) | [01](cypher/queries/interactive-complex-1.cypher) | [02](cypher/queries/interactive-complex-2.cypher) | [03](cypher/queries/interactive-complex-3.cypher) | [04](cypher/queries/interactive-complex-4.cypher) | [05](cypher/queries/interactive-complex-5.cypher) | [06](cypher/queries/interactive-complex-6.cypher) | [07](cypher/queries/interactive-complex-7.cypher) | [08](cypher/queries/interactive-complex-8.cypher) | [09](cypher/queries/interactive-complex-9.cypher) | [10](cypher/queries/interactive-complex-10.cypher) |
| OrientDB (SQL) | [01](postgres/queries/interactive-complex-1.sql)  | [02](postgres/queries/interactive-complex-2.sql)  | [03](postgres/queries/interactive-complex-3.sql)  | [04](postgres/queries/interactive-complex-4.sql)  | [05](postgres/queries/interactive-complex-5.sql)  | [06](postgres/queries/interactive-complex-6.sql)  | [07](postgres/queries/interactive-complex-7.sql)  | [08](postgres/queries/interactive-complex-8.sql)  | [09](postgres/queries/interactive-complex-9.sql)  | [10](postgres/queries/interactive-complex-10.sql)  |
| Spark (SQL)     | [01](postgres/queries/interactive-complex-1.sql)  | [02](postgres/queries/interactive-complex-2.sql)  | [03](postgres/queries/interactive-complex-3.sql)  | [04](postgres/queries/interactive-complex-4.sql)  | [05](postgres/queries/interactive-complex-5.sql)  | [06](postgres/queries/interactive-complex-6.sql)  | [07](postgres/queries/interactive-complex-7.sql)  | [08](postgres/queries/interactive-complex-8.sql)  | [09](postgres/queries/interactive-complex-9.sql)  | [10](postgres/queries/interactive-complex-10.sql)  |

## Running

Please follow the instructions in [wiki](https://github.com/Rucchao/UniBench_new/wiki) to run the benchmark.
