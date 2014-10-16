SQL search
==========

Setup
-----
1. Clone this repo
2. Install python, pip, elasticsearch, redis
3. pip install virtualenv
4. Instantiate a virtual environment
5. Run `pip install -r requirements.txt`
6. `ic.create(index='spire')`
7. `ic.put_mapping(index='spire', doc_type='query', body={"query":{"properties":{"start":{"type":"date"}}}})`
8. `ic.put_mapping(index='spire', doc_type='query', body={"query":{"properties":{"end":{"type":"date"}}}})`
9. `ic.put_mapping(index='spire', doc_type='query', body={"query":{"properties":{"date":{"type":"date", "format":"date"}}}})`

Starting the server
-------------------
1. Start Elasticsearch server `elasticsearch`
2. Start Redis Server `redis-server`
3. Start the celery worker `celery -A main.celery worker`
4. Run `python main.py`
