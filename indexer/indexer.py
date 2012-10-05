#!/usr/bin/env python
# file: indexer/index.py
# author: Aiman Najjar (an2434), Columbia University

"""
This module defines the main Indexer class which builds an inverted file
given a collection of documents. It may create files on the disk while
indexing documents and will raise IO errors if it fails in doing so.
"""
import logging
import threading
import re
import pickle
import math
import Queue
import cPickle
import zlib
from document import Document

class Zones:
    """Defines constants for index zones"""
    AUTHOR = 0
    TITLE = 1
    BIBLIO = 2
    TEXT = 3

class Indexer():
    """
    The Indexer class initializes multiple daemon threads to concurrently
    build an in-memory inverted file. When the client finishes feeding  
    the corpus to the indexer, it must inform the indexer by calling 
    close(), then the index may be built and saved using build_index()

        open() -- opens a new in-memory inverted file, if an existing file
                  is already open, it raises an exception. This overrides
                  the contents of the previously opened inddex file
        close() -- closes the in-memory inverted file, enqueue_document
                   can no longer be called after the file has been closed
        index_document() -- async call, enqueues a document to be
                                    indexed, open() must be called prior
                                    to be able to call index_document
        build_index() -- builds the index and save it to disk, close must
                         be called before attempting to build the index
        DELIMITERS -- A regular expression used to tokenize documents

    """

    DELIMITERS = '[\s.,=?!:@<>()\"-;\'&_\\{\\}\\|\\[\\]\\\\]+'    
    WEIGHT_FACTOR = [0,0,0,0]
    WEIGHT_FACTOR[Zones.AUTHOR] = 3.0    
    WEIGHT_FACTOR[Zones.TITLE] = 2.0
    WEIGHT_FACTOR[Zones.BIBLIO] = 1.5
    WEIGHT_FACTOR[Zones.TEXT] = 1.0

    def __init__(self, num_threads, enhanced_score=False):
        """
        Initialize and sart background threads.
            enhanced_score -- if set, give higher weights to metadata tokens
        """
        self._pass1_queue = Queue.Queue()
        self._pass2_queue = Queue.Queue()
        self._ifile_lock = threading.Lock()
        self._ifile_open = False
        self._corpus = []
        self._vector_space = []

        for i in range(num_threads):
            worker = threading.Thread(target=self._pass1,
                                      args=(i, self._pass1_queue,))
            worker.setDaemon(True)
            worker.start()              
            logging.debug("INDEXER-P1-THREAD-%d: Thread started." % i)

        for i in range(num_threads):
            worker = threading.Thread(target=self._pass2,
                                      args=(i, self._pass2_queue,))
            worker.setDaemon(True)
            worker.start()              
            logging.debug("INDEXER-P2-THREAD-%d: Thread started." % i)            

    def open(self):
        """
        Initialize an in-memory inverted file and override the contents 
        of last inverted file, raise an exception if one
        already exists
        """
        logging.debug("Opening inverted file")
        with self._ifile_lock:
            if self._ifile_open:
                logging.error("Cannot open a new index file before closing " \
                              "existing open index")
                raise Exception("An index file is already open")
            self._ifile = dict()
            self._ifile_open = True
        logging.debug("A new in-memory index has been opened.")

    def index_document(self, path_to_file):
        """Enqueue a document for indexing"""
        with self._ifile_lock:
            if not self._ifile_open:
                raise Exception("You must open an in-memory index first.")

        self._pass1_queue.put(path_to_file)
        logging.debug("Document '%s' enqueued" % path_to_file)

    def close(self):
        """Close the in-memory inverted file"""
        logging.debug("Closing inverted file, there are ~ %d pending documents"
                     " in queue " % self._pass1_queue.qsize())
        with self._ifile_lock:
            if not self._ifile_open:
                raise Exception("There is not an open inverted file in memory")
            self._ifile_open = False
        self._pass1_queue.join()            
        logging.debug("Inverted file closed")        

    def build_index(self):
        """Build the inverted file"""

        # open index file on disk
        f = open("./index.if", 'w')
        
        # Write metadata
        # len:total tokens|len:total documents        
        total_tokens = str(len(self._ifile))
        total_docs = str(len(self._corpus))
        len_total_tokens = str(len(total_tokens))
        len_total_docs = str(len(total_docs))
        f.write(len_total_tokens + ":" + total_tokens)
        f.write(len_total_docs + ":" + total_docs)

        # Write postings lists for each term
        postings_list_pointers = dict()  # maintains positions of terms 
                                         # postings lists in disk
        for term in sorted(self._ifile):
            liststr = ""
            for doc_id in self._ifile[term][2]:
                liststr = liststr + str(doc_id) + ","
            liststr = liststr.rstrip(",")
            # write postings list for the term in this format:
            # len_of_list:list (csv)
            postings_list_pointers[term] = f.tell() # position in the file for
                                                    # this term's list
            f.write(str(len(liststr)) + ":" + liststr)

        f.close()

        # Now we would like to 'pickle' the vocabulary but excluding
        # the postings list and store it in ifile.pickle
        # Also, we change the third dimension value to postings list pointer
        # on disk file ./index.if
        for term in sorted(self._ifile):
            self._ifile[term][1] = math.log( float(len(self._corpus)) / 
                                             float(len(self._ifile[term][2]))) # idf
            self._ifile[term][2] = postings_list_pointers[term]

        f = open("ifile.pickle", "w")
        # pickle.dump(self._ifile, f, pickle.HIGHEST_PROTOCOL)
        f.write(zlib.compress(cPickle.dumps(self._ifile,cPickle.HIGHEST_PROTOCOL),9))
        f.close()

        logging.info("Index files created")

        # Initialize vector space
        self._vector_space = [None] * len(self._corpus)

        # Dispatch documents to second pass
        # for document in self._corpus:
            # self._pass2_queue.put(document)

        # Wait for vector space to be built
        # self._pass2_queue.join()

        # Dump the file
        # logging.info("Saving index to disk")
        
        

    ## Private Methods ##

    # Background threads loop, pass 2
    def _pass1(self, thread_no, queue):
        while True:
            logging.debug("INDEXER-P1-THREAD-%d: Waiting for next document" %
                          thread_no)
            document_path = queue.get() # blocks until a document is avialable
            

            document = Document.from_file(document_path)
            if document is None:
                logging.warning("INDEXER-P1-THREAD-%d: Document %s contains "
                                "invalid format" % (thread_no,document_path))
                continue
            logging.debug("INDEXER-P1-THREAD-%d: Processing '%s'" % 
                            (thread_no,document.title))

            # Assign an ID to the document
            with self._ifile_lock:
                document.document_id = len(self._corpus)
                self._corpus.append(document)

            # Tokenize
            tokens = re.compile(Indexer.DELIMITERS).split(document.text)
            tokens_title = re.compile(Indexer.DELIMITERS).split(document.title)
            tokens_author = re.compile(Indexer.DELIMITERS).split(document.author)
            tokens_biblio = re.compile(Indexer.DELIMITERS).split(document.biblio)
            

            # Insert tokens in inverted files
            for token in tokens:                
                self._pass1_process_token(document.document_id, token)

            for token in tokens_title:                
                self._pass1_process_token(document.document_id, token)

            for token in tokens_author:
                self._pass1_process_token(document.document_id, token)

            for token in tokens_biblio:
                self._pass1_process_token(document.document_id, token)

            queue.task_done()

    def _pass1_process_token(self, doc_id, token):
        # Inverted file structure:
        # self._ifile[token] = [idf, list of doc ids]
        with self._ifile_lock:                    
            if self._ifile is None:
                logging.error("INDEXER-P1-THREAD-%d: Attempting to index"
                              " a document while index file is closed"
                              % thread_no)
                raise Exception("Index file has been closed")

            token = token.lower()
            if token not in self._ifile:
                self._ifile[token] = [0,0.0,set()]
            self._ifile[token][2].add(doc_id)


    # Background threads loop, pass 2
    def _pass2(self, thread_no, queue):
        while True:
            logging.debug("INDEXER-P2-THREAD-%d: Waiting for next document" %
                          thread_no)

            document = queue.get() # blocks until a document is avialable

            logging.debug("INDEXER-P2-THREAD-%d: Processing '%s'" % 
                            (thread_no,document.title))


            # Process document tokens
            tokens = re.compile(Indexer.DELIMITERS).split(document.text)
            tokens_title = re.compile(Indexer.DELIMITERS).split(document.title)
            tokens_author = re.compile(Indexer.DELIMITERS).split(document.author)
            tokens_biblio = re.compile(Indexer.DELIMITERS).split(document.biblio)
            
            # Initialize vector space for this document
            self._vector_space[document.document_id] = [0.0] * len(self._ifile)

            for token in tokens:                
                self._pass2_process_token(document.document_id, Zones.TEXT, token)

            for token in tokens_title:                
                self._pass2_process_token(document.document_id, Zones.TITLE, token)                

            for token in tokens_author:
                self._pass2_process_token(document.document_id, Zones.AUTHOR, token)                                

            for token in tokens_biblio:
                self._pass2_process_token(document.document_id, Zones.BIBLIO, token)                   

            queue.task_done()


    def _pass2_process_token(self, doc_id, zone, token):
        # Vector space structure
        # self._vector_space[d][t] = frequency of term t in document d
        # Ensure token is in lowercase
        token = token.lower()
        # Find term's index in vector space
        if token not in self._ifile:
            return
        t = self._ifile[token][0]
        self._vector_space[doc_id][t] = self._vector_space[doc_id][t] + 1.0


