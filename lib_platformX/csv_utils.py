"""
Utilities to handle reading and writing of Unicode data from and to CSV files
UTF8Recoder: iterator that reads an encoded stream and reencodes the input
to UTF-8
UnicodeReader: a CSV reader which will iterate over lines in the specified CSV
file
UnicodeWriter: a CSV writer which will write rows to the specified CSV file
All these utilities, plus relevant comments, are taken verbatim from
https://docs.python.org/2/library/csv.html, with
some minor tweaks.
"""
import codecs
from io import StringIO
from defusedcsv import csv


# For all other encodings the following UnicodeReader and UnicodeWriter
# classes can be used. They take an additional
# encoding parameter in their constructor and make sure that the data passes
# the real reader or writer encoded as UTF-8


class UTF8Recoder(object):
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """

    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode('utf-8')

    def __next__(self):
        """
        Once executed in PY3 this will be called and next() will be ignored as
        next() has been renamed to __next__()
        Since we will be running the update to PY and to avoid breaking
        everything, this was already added to the class
        """
        return self.reader.__next__()


class UnicodeFileObjectReader(object):
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwargs):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwargs)

    def next(self):
        row = self.reader.next()
        return [str(s, 'utf-8') for s in row]

    def __next__(self):
        """
        Once executed in PY3 this will be called and next() will be ignored as
        next() has been renamed to __next__()
        Since we will be running the update to PY and to avoid breaking
        everything, this was already added to the class
        """
        return self.reader.__next__()

    def __iter__(self):
        return self


class UnicodeFileObjectWriter(object):
    """
    A CSV writer which will write rows to CSV file "f", which is encoded in
    the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding='utf-8', **kwargs):
        # Redirect output to a queue
        self.queue = StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwargs)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        # Updated wrt original: added test for empty input
        self.writer.writerow(
            [s.encode('utf-8') if s is not None else '' for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode('utf-8')
        # ... and re-encode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class UnicodeReader:
    """
    CSV reader supporting Unicode that will work for both Python 2 and Python
    3.
    Note: will only work if used as a context manager.
    Taken from http://python3porting.com/problems.html
    """

    def __init__(self, filename, dialect=csv.excel, encoding='utf-8', **kw):
        self.filename = filename
        self.dialect = dialect
        self.encoding = encoding
        self.kw = kw

    def __enter__(self):
        self.f = open(self.filename, 'rt', encoding=self.encoding, newline='')
        self.reader = csv.reader(self.f, dialect=self.dialect, **self.kw)
        return self

    def __exit__(self, type, value, traceback):
        self.f.close()

    def next(self):
        return next(self.reader)

    __next__ = next

    def __iter__(self):
        return self


class UnicodeWriter:
    """
    CSV writer supporting Unicode that will work for both Python 2 and
    Python 3.
    Note: will only work if used as a context manager.
    Taken from http://python3porting.com/problems.html
    """

    def __init__(self, filename, dialect=csv.excel, encoding='utf-8', **kw):
        self.filename = filename
        self.dialect = dialect
        self.encoding = encoding
        self.kw = kw

    def __enter__(self):
        self.f = open(self.filename, 'wt', encoding=self.encoding, newline='')
        self.writer = csv.writer(self.f, dialect=self.dialect, **self.kw)
        return self

    def __exit__(self, *exc):
        self.f.close()

    def writerow(self, row):
        self.writer.writerow(row)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)
