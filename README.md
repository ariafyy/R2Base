# R2Base
A cloud-native ranker-reader engine for unstructure data retrieval

## Overview

R2Base provides a robust engine that implements modern neural ranker + machine reader architecture. R2Base is the future of universal search and question answering solutions, where data of all types can be queried with natural langauge with unprecendent precision. 

## Main Components
R2Base is composed of 4 major components. 

### Indexer
The indexer is a RESTful API and distributed workers that convert unstructured data into dense or sparse vectors, and upload them to the index. 

### Index
R2Base supports a range of different index that enables fast ranking and supports both text and visual data.
- Sparse Index: BM25 or SPARTA 
- Dense Index: Brtual Force or LSH

### Readers
R2Base uses Transformers as its reader. It can read both text and visual data.   

### Query Engine
The query engine is a server that enables clients to query R2Base via simple RESTful API. The query usually combines index ranking and reading.
