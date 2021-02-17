# R2Base (Ranking & Reducing Engine)
R2Base enables one to easily rank and reduce (dimension reduction and clustering) of high-dimensional dense/sparse vectors.


### Install & Run
    # install dependencies
    pip install -r requirements.txt
    
    # run ES
    docker pull docker.elastic.co/elasticsearch/elasticsearch:7.10.1
    docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.10.1
    
    # run API
    uvicorn r2base.http.server:app --host 0.0.0.0 --workers 4
   
Or you can use docker-compose:
    
    # CPU Mode
    docker-compose -f docker-compose.yml up -d
    
    # GPU mode
    docker-compose -f docker-compose-gpu.yml up -d

### Build Index
#### Support types
- datetime
- date
- int
- float
- text
- vector
- term_score


### Query Index
#### Basic Usage
- match one or more field

        {
            "query": {"match": {field": "value"}}
        }
- filter one or more field

        {
            "query": {"filter": "field=A OR field < B"}
        }
- combine match and filter

        {
            "query": {
                "match": {"field": "value"},
                "filter": "field=A OR field < B"
            }
        }

#### Advanced Usage

- match with threshold
- match text with advanced ES query
- scroll with query

### Examples
    Checkout examples/
    
### Live Documentation
    http://localhost:8000/docs

    
 

