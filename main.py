from os import path, environ
import json, csv, StringIO

from elasticsearch import Elasticsearch
import requests
from flask import Flask, Blueprint, abort, jsonify, request, session
from celery import Celery

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

### Celery Tasks ###

@celery.task(name="task.upload_hive_queries")
def upload_hive_queries(direct_link):
    r = requests.get(direct_link)
    reader = unicode_csv_reader(StringIO.StringIO(r.text))
    headers = reader.next()
    if headers == ['id', 'owner', 'name', 'parent_name', 'statement', 'start_ts', 'end_ts']:
        return headers
    else:
        return False

### Views ###

@app.route("/")
def main():
    res = upload_hive_queries.delay('https://dl.dropbox.com/s/je8k1xr9s2bk3w8/Hive%20Queries%20Sample.csv')
    return res.task_id

@app.route("/<task_id>")
def show_result(task_id):
    retval = upload_hive_queries.AsyncResult(task_id).get(timeout=1)
    return repr(retval)


if __name__ == "__main__":
    app.run(debug=True)
