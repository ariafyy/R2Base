# R2Base: AI search engine

### What is R2Base?
R2Base a document store with semantic search and machine reading

### Examples

Checkout
    
    example.py
    wiki_example.py
    
### Run Server
    uvicorn r2base.http.server:app --host 0.0.0.0 --workers 4


### TODO

- Support basic operations, e.g. CRUD, filters etc [DONE]
- Support native SPARTA in Tantivy
- Support vector search using FASISS
- Support reader 
- Add better RESTful APIs [DONE]
- Package as Docker image
- Use ONNX instead of pytorch for models
- Use faster BERT tokenizer 
    
 

