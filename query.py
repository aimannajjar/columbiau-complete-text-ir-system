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
import datetime
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
    qe = None
    try:
        qe = QueryEngine(MAX_RESULTS)
    except Exception, e:
        print ''
        print "Could not initalize query engine"
        print ''
        sys.exit(-1)

    
    print ''
    print '------------------------------------------' 
    print ' Columbia University - Fall 2012'
    print ' Search Engine Technology Class - Basic IR System'
    print ' Done by: Aiman Najjar (an2434)'
    print '------------------------------------------' 

    print ' Welcome! Please note that this interactive client\n' \
          ' of the query engine returns only the top 5 matches\n' \
          ' this can be changed by tweaking MAX_RESULTS constant' 
    print ''

    
    while (True):
        sys.stdout.write('Query: ')
        value = raw_input()
        query_obj = Query.from_string(value)

        a = datetime.datetime.now()
        results = qe.query(query_obj)
        b = datetime.datetime.now()

        i = 0

        
        if query_obj.cmd == "similar":
            print ''
            print 'Words similar to %s in context:' % query_obj.groups[0]
            sys.stdout.write(' ')
            sys.stdout.write("\n ".join(results))
            print ''

        elif query_obj.cmd == "df":
            print ''
            print 'Document Frequency of %s in corpus: %d ' % \
                    (query_obj.groups[0], results)

        elif query_obj.cmd == "freq":
            print ''
            print 'Total Frequency of %s in corpus: %d ' % \
                    (query_obj.groups[0], results)

        elif query_obj.cmd == "doc" or query_obj.cmd == "title":
            doc_id = 0
            if len(query_obj.raw_terms) < 1:
                print 'Please specify document ID'
                print ''
            try:
                doc_id = int(query_obj.raw_terms[0])
                if doc_id in qe.corpus:
                    print ''

                    if query_obj.cmd == "doc":
                        print '%-9s: %-10s' % ("DocNo", doc_id)                        
                        print '%-9s: %10s' % ("Title", qe.corpus[doc_id].title)
                        print '%-9s: %10s' % ("Author", qe.corpus[doc_id].author)
                        print ''
                        print qe.corpus[doc_id].original_text
                    else:
                        print "Title: %s " % qe.corpus[doc_id].title

                else:
                    print 'There is no document with specified ID'

            except Exception, e:
                print 'Invalid document ID'

        elif query_obj.cmd == "tf":
            print 'Term Frquency in Document %s: %d' % \
                (query_obj.raw_terms[0], results)
        
        else:            
            for doc in results:
                doc_id = doc[0]
                pos = doc[2] - 3
                if pos < 0:
                    post = 0
                print ''                    
                print 'Result %d' % (i+1)
                print '['
                print '  %-9s: %-10s' % ("DocNo", doc_id)                        
                print '  %-9s: %10s' % ("Title", qe.corpus[doc_id].colored_title(query_obj.raw_terms))
                print '  %-9s: %10s' % ("Author", qe.corpus[doc_id].colored_author(query_obj.raw_terms))
                print '  %-9s: %10s' % ("Summary", qe.corpus[doc_id].text_snippet(query_obj.raw_terms, pos, 20))
                print ']'

                i = i + 1

        print ''
        print 'Query executed in %0.4f seconds' % ( ( (b-a).microseconds / 1000.0) / 1000.0 )
        print '------------------------------------------'
        print ''

