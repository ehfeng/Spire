from flask import Flask, request, render_template, abort, url_for, redirect, \
    current_app, flash, session, make_response
from elasticsearch import Elasticsearch

app = Flask(__name__)
es = Elasticsearch()

# Query object

@app.route("/")
def main():
	return render_template('main.html')

@app.route("/import")
def importer():
	return render_template('importer.html')

if __name__ == "__main__":
	app.debug = True
	app.run()