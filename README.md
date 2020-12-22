# R2Base: AI search engine

### What is R2Base?
R2Base a document store with semantic search and machine reading

### Examples
Checkout
    query_tty.py
    wiki_example.py
    
### Run Server
    uvicorn r2base.http.server:app --host 0.0.0.0 --workers 4

### Dependencies
    Most of the time, you need to run Elasticsearch as backend for production usage.

### TODO

- Support basic operations, e.g. CRUD, filters etc [DONE]
- Support native SPARTA in Tantivy [DONE]
- Support vector search using FASISS [DONE]
- Support reader 
- Add better RESTful APIs [DONE]
- Package as Docker image [DONE]
- Use ONNX instead of pytorch for models 
- Use faster BERT tokenizer 
    
 

