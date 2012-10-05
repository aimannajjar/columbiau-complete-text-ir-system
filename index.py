#!/usr/bin/env python
# file: index.py
# author: Aiman Najjar (an2434), Columbia University

"""
Usage: python index.py /path/to/data

This script indexes a collection of documents and outputs a binary file
of the invertedFile. The output file includes other metadata related to
the corpus, such as vector representations of the documents.

The documents that need to be indexed must adhere to the following XML
structure:

    <DOC>
    <DOCNO>
    ...
    </DOCNO>
    <TITLE>
    ...
    </TITLE>
    <AUTHOR>
    ...
    </AUTHOR>
    <BIBLIO>
    ...
    </BIBLIO>
    <TEXT>
    ...
    </TEXT>
    </DOC>

Doucments that are not correclty formatted will not be indexed and a 
warning message will be issued to stderr

    You can tweak the following constants to manipulate the indexer
    settings:

    NUM_INDEXER_THREADS --  how many concurrent threads should the
                            indexer spawn

"""
import sys
import os
import logging
from indexer.indexer import Indexer

# Constants
NUM_INDEXER_THREADS = 2


# Prints Usage
def usage():
    print "usage: python index.py /path/to/data"

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    
    args = sys.argv
    if len(args) < 2 or len(args) > 2:
        usage()    
        sys.exit()

    indexer = Indexer(NUM_INDEXER_THREADS)
    indexer.open()

    logging.info("Starting pass 1")
    for filename in os.listdir(args[1]):
        indexer.index_document(os.path.join(args[1], filename))
    indexer.close()
    logging.info("Pass 1 done")

    logging.info("Starting pass 2")
    indexer.build_index()
    logging.info("Pass 2 done")


