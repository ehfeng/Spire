from queryparser import queryToParse
from main import Query
from pyparsing import ParseException
import csv, codecs, cStringIO

class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def test():
    results = Query.search(size=10000)['hits']['hits']

    worked_file = open('working/worked.csv', 'w')
    failed_file = open('working/failed.csv', 'w')
    worked_csv = UnicodeWriter(worked_file)
    failed_csv = UnicodeWriter(failed_file)
    worked=0
    failed=0

    for result in results:
        query_str = result['_source']['statement'].encode('utf-8', errors='replace')
        try:
            queryToParse.parseString(query_str)
            worked += 1
            worked_csv.writerow([result['_id'], result['_source']['statement']])
        except:
            failed += 1
            failed_csv.writerow([result['_id'], result['_source']['statement']])
    print "Total worked: " + str(worked)
    print "Total failed: " + str(failed)


if __name__ == "__main__":
    test()