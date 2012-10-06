#!/usr/bin/env python
# file: query_engine/engine.py
# author: Aiman Najjar (an2434), Columbia University

"""
Defines the class responsible for loading main index file into
memory and perform queries on disk as well as memory
"""
import cPickle
import zlib
import json

class QueryEngine():
    """
    The query engine loads the inverted file dictionary into memory
    however, it leaves the postings list on disk and only accesses them
    when needed. The inverted file dictionary has pointers to the 
    postings lists positions on disk file. For more details on index
    format, see Indexer.build_index() in indexer.indexer mobule
    """
    def __init__(self, index_name="ifile"):
        """ Load main index into memory"""
        self._ifile = None
        self._vector_space = None
        f = open("%s.dict" % index_name, "r")
        zstr = f.read()
        f.close()

        self._ifile = cPickle.loads(zlib.decompress(zstr))

        f = open("%s.vs" % index_name, "r")
        zstr = f.read()
        f.close()
        self._vector_space = cPickle.loads(zlib.decompress(zstr))

        self._postings_file = open("%s.postings" % index_name, "r")

    def query(self, query):
        """
        Perform specified query and return results
            query -- search query, examples of search queries are:            

                cat             : returns any document that has the word
                                  "cat" in it
                cat dog         : any document that has one or more of 
                                  these words
                                 ("fuzzy or" is assumed by default)
                cat dog rat     : up to 10 words in a query
                "tabby cat"     : phrases of up to 5 words in length
                "small tabby cat" "shaggy dog" : multiple phrases in a 
                                                 query
                !cat !"tabby cat": negations of single words or phrases
                !cat !dog       : multiple negations per query

                Note that the default Boolean operator ("fuzzy or") 
                above is an extension to "AND" and "OR".
        """
        query_terms = query.split(" ")

        results = None
        scores = dict()
        for term in query_terms:
            if term in self._ifile:

                ## Determine list size:
                self._postings_file.seek(self._ifile[term][2])
                c = self._postings_file.read(1)
                list_size = ""
                while c != ":":
                    list_size = list_size + c
                    c = self._postings_file.read(1)

                postings_list = set(json.loads("[" + self._postings_file.read(int(list_size)) +"]"))
                if results is None:
                    results = postings_list
                else:
                    results = results.intersection(postings_list)


        i = 0 
        
        if results is None or len(results) <= 0:
            return

        for term in query_terms:
            term_index = self._ifile[term][0]
            for doc in results:
                if not doc in scores:
                    scores[doc] = 0.0
                scores[doc] = scores[doc] + self._ifile[term][1] * self._vector_space[doc-1][term_index] 
                if i > 10:
                    break

        for doc in sorted(scores, key=lambda d: scores[d], reverse=True):
            print "Doc %s - Score: %s" % (doc, scores[doc])




