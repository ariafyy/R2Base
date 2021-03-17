# R2Base: Vector Engine

### What is R2Base (Rank & Reducing Base)?
R2Base is a vector database for ranking, reducing millions of dense (sparse) vectors.

### Examples
Checkout
    query_tty.py
    wiki_example.py
    
### Run Server
    uvicorn r2base.http.server:app --host 0.0.0.0 --workers 4

### Dependencies
    Most of the time, you need to run Elasticsearch as backend for production usage.

### Run for Development
    docker pull docker.elastic.co/elasticsearch/elasticsearch:7.10.1
    docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.10.1
    uvicorn r2base.http.server:app --host 0.0.0.0 --workers 4

### Live Docs
    http://localhost:8000/docs

### TODO
- Support reader 
- Use ONNX instead of PyTorch for models 
- Improve stabtility 
- Use faster BERT tokenizer 
- Support basic operations, e.g. CRUD, filters etc [DONE]
- Support native SPARTA in Tantivy [DONE]
- Support vector search using FASISS [DONE]
- Add better RESTful APIs [DONE]
- Package as Docker image [DONE]
- Support BM25 for CJK languages [DONE for Chinese]
    
 

