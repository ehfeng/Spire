SQL search
==========

Setup
-----
1. Clone this repo
2. Install python, pip, elasticsearch, redis
3. pip install virtualenv
4. Instantiate a virtual environment
5. Run `pip install -r requirements.txt`
6. Start Elasticsearch server `elasticsearch`
7. Start Redis Server `redis-server`
8. Start the celery worker `celery -A main.celery worker`
9. Run `python main.py`
