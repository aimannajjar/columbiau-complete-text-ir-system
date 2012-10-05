#!/usr/bin/env python
# file: index.py
# author: Aiman Najjar (an2434), Columbia University

"""
Usage: python query.py

The script evaluate and process queries on the index created by indexer
script. Index files must reside in the same Current Working Directory
(CWD): index.if and ifile.pickle



"""
import sys
import os
import logging
from indexer.indexer import Indexer

# Constants
NUM_INDEXER_THREADS = 2


# Prints Usage
def usage():
    print "usage: python query.py"

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    
    QueryEngine qe = QueryEngine()
    logging.info("Loading index file")

    
    sys.stdout.write('Query: ')
    value = raw_input()

