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
import math
import Queue
import cPickle
import zlib
import constants
from document.document import Document
from document.PorterStemmer import PorterStemmer
from document.document import Zones

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

    """

    def __init__(self, num_threads, index_name="ifile", enhanced_score=False):
        """
        Initialize and sart background threads.
            enhanced_score -- if set, give higher weights to metadata tokens
        """
        self._pass1_queue = Queue.Queue()
        self._pass2_queue = Queue.Queue()
        self._ifile_lock = threading.Lock()
        self._ifile_open = False
        self._corpus = {}
        self._vector_space = []
        self.index_name = index_name

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

        self._full_text = None
        try:
            import nltk
            import nltk.text
            self._full_text = ""            
        except Exception, e:
            logging.warning("You don't have nltk module installed. Similar queries will not be supported")


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
        f = open("%s.postings" % self.index_name, 'w')
        
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
            last_doc_id = 0
            for doc_id in sorted(self._ifile[term][2]):
                liststr = liststr + str(int(doc_id) - last_doc_id) + ","
                last_doc_id = doc_id
            liststr = liststr.rstrip(",")
            # write postings list for the term in this format:
            # len_of_list:list (csv)
            postings_list_pointers[term] = f.tell() # position in the file for
                                                    # this term's list
            f.write(str(len(liststr)) + ":" + liststr)

        f.close()

        # Now we would like to 'pickle' the vocabulary but excluding
        # the postings list and store it in filename.dict
        # Also, we change the third dimension value to postings list pointer
        # on disk file filename.postings
        i = 0
        for term in sorted(self._ifile):
            self._ifile[term][0] = i
            self._ifile[term][1] = len(self._ifile[term][2]) # df
            self._ifile[term][2] = postings_list_pointers[term]
            i = i + 1

        f = open("%s.dict" % self.index_name, "w")
        f.write(zlib.compress(cPickle.dumps(self._ifile,cPickle.HIGHEST_PROTOCOL),9))
        f.close()

        # Initialize vector space
        self._vector_space = [None] * (len(self._corpus) + 1)

        # Dispatch documents to second pass
        for doc_id in self._corpus:
            self._pass2_queue.put(self._corpus[doc_id])

        # Wait for vector space to be built
        self._pass2_queue.join()

        # Dump the vector space
        f = open("%s.vs" % self.index_name, "w")
        f.write(zlib.compress(cPickle.dumps(self._vector_space,cPickle.HIGHEST_PROTOCOL),9))
        f.close()
        
        # And finally dump corpus
        f = open("%s.corpus" % self.index_name, "w")
        f.write(zlib.compress(cPickle.dumps(self._corpus,cPickle.HIGHEST_PROTOCOL),9))
        f.close()

        # Construct similar index
        if self._full_text is not None:
            import nltk
            import nltk.text            
            corpus_text = nltk.text.Text([word.lower() for word in nltk.word_tokenize(self._full_text)])

            # Dump context index
            f = open("%s.context" % self.index_name, "w")
            f.write(zlib.compress(cPickle.dumps(corpus_text,cPickle.HIGHEST_PROTOCOL),9))
            f.close()

    ## Private Methods ##

    # Background threads loop, pass 1
    def _pass1(self, thread_no, queue):
        while True:
            logging.debug("INDEXER-P1-THREAD-%d: Waiting for next document" %
                          thread_no)
            document_path = queue.get() # blocks until a document is avialable
            

            document = Document.from_file(document_path)
            if document is None:
                logging.warning("INDEXER-P1-THREAD-%d: Document %s contains "
                                "invalid format" % (thread_no,document_path))
                queue.task_done()
                continue
            logging.debug("INDEXER-P1-THREAD-%d: Processing '%s'" % 
                            (thread_no,document.title))

            # Assign an ID to the document
            with self._ifile_lock:
                document.document_id = int(document.document_number)
                self._corpus[document.document_id] = document

                # For similar context index
                if (self._full_text is not None):
                    cleaned_text = []
                    for word in document.text.split(" "):
                        if not word.lower() in constants.DO_NOT_INDEX:
                            cleaned_text.append(word)
                    self._full_text = self._full_text + " " + " ".join(cleaned_text)


            # Tokenize
            tokens = re.compile(constants.DELIMITERS).split(document.text)
            tokens_title = re.compile(constants.DELIMITERS).split(document.title)
            tokens_author = re.compile(constants.DELIMITERS).split(document.author)
            tokens_biblio = re.compile(constants.DELIMITERS).split(document.biblio)


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
        # self._ifile[token] = [id, df, postings_list]

        # Let's make sure the token is not in our "do not index"

        if token in constants.DO_NOT_INDEX or len(token) <= 1:
            return
        p = PorterStemmer()
        # First, let's stem the token
        token = token.lower()
        token = p.stem(token.lower(), 0,len(token)-1)

        with self._ifile_lock:                    
            if self._ifile is None:
                logging.error("INDEXER-P1-THREAD-%d: Attempting to index"
                              " a document while index file is closed"
                              % thread_no)
                raise Exception("Index file has been closed")

            if token not in self._ifile:
                self._ifile[token] = [0,0,set()]
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
            tokens = re.compile(constants.DELIMITERS).split(document.text)
            tokens_title = re.compile(constants.DELIMITERS).split(document.title)
            tokens_author = re.compile(constants.DELIMITERS).split(document.author)
            tokens_biblio = re.compile(constants.DELIMITERS).split(document.biblio)
            
            # Initialize vector space for this document
            self._vector_space[document.document_id] = dict()
            

            curr_pos = 1
            for token in tokens:                
                self._pass2_process_token(document, curr_pos, Zones.TEXT,
                                          token)
                curr_pos = curr_pos + 1                

            curr_pos = 1
            for token in tokens_title:                
                self._pass2_process_token(document, curr_pos, Zones.TITLE, 
                                          token)                
                curr_pos = curr_pos + 1                

            curr_pos = 1
            for token in tokens_author:
                self._pass2_process_token(document, curr_pos, Zones.AUTHOR,
                                          token)                                
                curr_pos = curr_pos + 1                

            curr_pos = 1
            for token in tokens_biblio:
                self._pass2_process_token(document, curr_pos, Zones.BIBLIO,
                                          token)  
                curr_pos = curr_pos + 1                

            queue.task_done()


    def _pass2_process_token(self, document, position, zone, token):
        # Vector space structure
        # vector_space[d][t][0] = normalized frequency of term t in document d
        # vector_space[d][t][1] = positions of term t in document d for each zone
        # vector_space[d][t][2] = frequency of term t in document d
        # positions are in this format: ZoneNumber | position
        # Ensure token is in lowercase and eligible for index
        if token in constants.DO_NOT_INDEX or len(token) <= 1:
            return

        p = PorterStemmer()
        # First, let's stem the token
        token = token.lower()
        token = p.stem(token.lower(), 0,len(token)-1)

        # Find term's index in vector space
        if token not in self._ifile:
            return
        t = self._ifile[token][0]
        if not t in self._vector_space[document.document_id]:
            self._vector_space[document.document_id][t] = [0.0, [[],[],[],[]],0]

        self._vector_space[document.document_id][t][0] = \
            (Zones.WEIGHTS[zone] / document.weighted_length) \
            + self._vector_space[document.document_id][t][0]

        self._vector_space[document.document_id][t][1][zone].append(position)
        self._vector_space[document.document_id][t][2] += 1


