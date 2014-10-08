from flask import Flask
from elasticsearch import Elasticsearch

app = Flask(__name__)
es = Elasticsearch()

# Query object

@app.route("/")
def main():
	return "Hello World!"

if __name__ == "__main__":
	app.debug = True
	app.run()