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
- Dense Index: Brutal Force or LSH

### Readers
R2Base uses Transformers as its reader. It can read both text and visual data.   

### Query Engine
The query engine is a server that enables clients to query R2Base via simple RESTful API. The query usually combines index ranking and reading.

## What is not R2Base
We try to keep R2Base simple and focused. R2Base is not responsible for:

- extracting the information from raw files, e.g. PDFs or WAV
- chunking the data into meaningful semantic units, e.g. paragraphs or sentences. 
- which model is used for indexing or reading. R2Base only cares about executing these models
- other types of annotations of the data, which can be provided as meta as is

## What is R2Base

R2Base focuses on providing a robust backbone for ranking and reading. It provides:
- It provides a clear config query that can be used to reproduce a strategy of using ranking & reading for any applications.
- It is optimized for scale and can be scaled up by adding more nodes. 
- It is optimized for speed where unnecessary delays are all taken care of.
- It is optimized for robustness so it's not easy to crash or slow down the system.


## How is R2Base related to SocoDB
R2Base is the base of SocoDB, where SocoDB builds on top of it.
- SocoDB provides additional annotators and also provides aggregation functions. 
- SocoDB also works on top of the **Task** where R2Base works on top of **Index**.
- SocoDB uses query/admin key to secure each task and R2Base only has a system level key.
- SocoDB is not open where as R2Base may become an open source project. 
- SocoDB has built in models for different languages and modality, and R2Base does not have any models. It only executes them.









