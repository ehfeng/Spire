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
3. Start the celery worker `celery -A main.celery worker` in virtual environment
4. Run `python main.py` in virtual environment

Getting the data
----------------
select
  a.id as id,
  a.owner as owner,
  a.name as name,
  b.name as parent_name,
  a.statement as statement,
  a.start_ts as start_ts,
  a.heartbeat_ts as end_ts
from (
  select id, parent, owner, name, statement, start_ts, heartbeat_ts from hp_query where result=0 and parent is not null and statement not like 'explain %' and start_ts>=unix_timestamp($DATE(-28), 'yyyy-MM-dd')
) a join (
  select id, name from hp_query where parent is null
) b on a.parent=b.id