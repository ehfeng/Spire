from os import path, environ
import json, csv, StringIO

from elasticsearch import Elasticsearch
import requests
from flask import Flask, Blueprint, abort, jsonify, request, session
from celery import Celery

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

@celery.task(name="task.upload_hive_queries")
def upload_hive_queries(direct_link):
    print "starting download"
    r = requests.get(direct_link)
    print "finished download"
    reader = csv.reader(StringIO.StringIO(r.text))
    headers = reader.next()
    for row in reader:
        print row
    return headers

@app.route("/")
def main():
    res = upload_hive_queries.delay('https://dl.dropboxusercontent.com/1/view/gi5xw1i74y4gvmr/Apps/Intercom%20Folder/weekly_actives/2014-10-05.csv')
    return res.task_id

@app.route("/<task_id>")
def show_result(task_id):
    retval = upload_hive_queries.AsyncResult(task_id).get(timeout=1)
    return repr(retval)


if __name__ == "__main__":
    app.run(debug=True)
