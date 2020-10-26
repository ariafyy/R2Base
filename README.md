# R2Base: search engine for unstructured data

## What is R2Base?
R2Base a document store with semantic search and machine reading

## Get Started

    indexer.create_index('helloworld', {text: {type: text, index: bert}})
    doc_id = indexer.add_doc(index, [doc1, doc2])
    ranker.query(index, {'query': {text: china}}))

