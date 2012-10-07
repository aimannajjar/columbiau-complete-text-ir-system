#!/usr/bin/env python
# file: index.py
# author: Aiman Najjar (an2434), Columbia University

"""
Usage: python query.py

The script evaluate and process queries on the index created by indexer
script. Index files must reside in the same Current Working Directory
(CWD): ifile.dict, ifile.postings, ifile.vs



"""
import sys
import os
import logging
from query_engine.engine import QueryEngine
from query_engine.query import Query

# Constants
NUM_INDEXER_THREADS = 2
MAX_RESULTS = 5

# Prints Usage
def usage():
    print "usage: python query.py /path/to/data_collection"

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    
    logging.info("Loading index file")
    qe = QueryEngine(MAX_RESULTS)
    

    
    while (True):
        sys.stdout.write('Query: ')
        value = raw_input()
        results = qe.query(Query.from_string(value))
        i = 0
        for doc in results:
            doc_id = doc[0]
            pos = doc[2] - 3
            if pos < 0:
                post = 0
            print 'Result %d' % (i+1)
            print '['
            print '  %-9s: %-10s' % ("DocNo", doc_id)                        
            print '  %-9s: %10s' % ("Title", qe.corpus[doc_id-1].title)
            print '  %-9s: %10s' % ("Author", qe.corpus[doc_id-1].author)
            print '  %-9s: %10s' % ("Summary", qe.corpus[doc_id-1].text_snippet(pos, 20))
            print ']'
            print ''
            i = i + 1


