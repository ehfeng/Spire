from os import environ, path
import csv, datetime, json, StringIO

from celery import Celery
from elasticsearch import Elasticsearch
from flask import abort, Blueprint, Flask, jsonify, render_template, request, session
import requests

from queryparser import queryToParse

### Constants ###

ES_INDEX="spire"

### Helper Functions ###

def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')

def make_celery(app):
    celery = Celery(app.import_name, backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')
    celery.conf.update(app.config)
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

app = Flask(__name__)
celery = make_celery(app)
es = Elasticsearch()

### Elasticsearch helpers ###

class Query:
    doc_type="query"

    @classmethod
    def exists(cls, **kwargs):
        return es.exists(index=ES_INDEX, doc_type=cls.doc_type, **kwargs)

    @classmethod
    def get(cls, id=id, **kwargs):
        return es.get(index=ES_INDEX, doc_type=cls.doc_type, id=id, **kwargs)

    @classmethod
    def create(cls, **kwargs):
        return es.create(index=ES_INDEX, doc_type=cls.doc_type, **kwargs)

    @classmethod
    def update(cls, **kwargs):
        return es.update(index=ES_INDEX, doc_type=cls.doc_type, **kwargs)

    @classmethod
    def delete(cls, **kwargs):
        return es.delete(index=ES_INDEX, doc_type=cls.doc_type, **kwargs)

    @classmethod
    def search(cls, **kwargs):
        if not kwargs.has_key('size'):
            kwargs['size']=100
        if not kwargs.has_key('default_operator'):
            kwargs['default_operator']='AND'
        return es.search(index=ES_INDEX, doc_type=cls.doc_type, **kwargs)

### Celery Tasks ###

@celery.task(name="task.upload_hive_queries")
def upload_hive_queries(direct_link):
    r = requests.get(direct_link)
    reader = unicode_csv_reader(StringIO.StringIO(r.text))
    headers = reader.next()
    if headers == ['id', 'owner', 'name', 'parent_name', 'statement', 'start_ts', 'end_ts']:
        for row in reader:
            qid=int(row[0])
            if not Query.exists(id=qid):
                query_kwargs={}
                query_kwargs['owner']=row[1]
                query_kwargs['name']=row[2]
                query_kwargs['parent']=row[3]
                query_kwargs['statement']=row[4].replace('?', '\n')
                query_kwargs['start']=datetime.datetime.fromtimestamp(int(float(row[5]))).isoformat()
                query_kwargs['end']=datetime.datetime.fromtimestamp(int(float(row[6]))).isoformat()
                query_kwargs['date']=datetime.datetime.fromtimestamp(int(float(row[5]))).strftime('%Y-%m-%d')
                Query.create(id=qid, body=query_kwargs)
        return True

@celery.task(name="task.parse_hive_queries")
def parse_hive_queries():
    return None


### Views ###

@app.route("/", methods=["GET", "POST"])
def main():
    if request.method == "POST":
        q=request.form['q']
        results=Query.search(q=q)['hits']['hits']
        queries = []
        for result in results:
            query = {'id': result['_id']}
            query.update(result['_source'])
            queries.append(query)
        return render_template('main.html', q=q, queries=queries)
    return render_template('main.html')

@app.route("/upload")
def upload():
    res = upload_hive_queries.delay('https://dl.dropbox.com/s/kgd0map2xpqytsl/Hive%20Queries.csv')
    return res.task_id

@app.route("/upload/<task_id>")
def upload_result(task_id):
    task = upload_hive_queries.AsyncResult(task_id)
    if task.ready():
        # retval = task.get(timeout=1)
        return repr(True)
    else:
        return repr(False)


if __name__ == "__main__":
    app.run(debug=True)
