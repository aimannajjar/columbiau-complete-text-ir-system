#!/usr/bin/env python
# file: query_engine/engine.py
# author: Aiman Najjar (an2434), Columbia University

"""
Defines the class responsible for loading main index file into
memory and perform queries on disk as well as memory
"""
import zlib
import json
import math
import logging
import sys
import cPickle
from document.document import Zones


class QueryEngine():
    """
    The query engine loads the inverted file dictionary into memory
    however, it leaves the postings list on disk and only accesses them
    when needed. The inverted file dictionary has pointers to the 
    postings lists positions on disk file. For more details on index
    format, see Indexer.build_index() in indexer.indexer mobule
    """
    def __init__(self, max_results=15, index_name="ifile"):
        """ Load main index into memory"""
        self._ifile = None
        self._vector_space = None
        self.max_results = max_results
        f = open("%s.dict" % index_name, "r")
        zstr = f.read()
        f.close()

        self._ifile = cPickle.loads(zlib.decompress(zstr))

        f = open("%s.vs" % index_name, "r")
        zstr = f.read()
        f.close()
        self._vector_space = cPickle.loads(zlib.decompress(zstr))

        self._postings_file = open("%s.postings" % index_name, "r")

        # ----------------------------------
        # Read meta data from postings file
        # 1. Read total tokens
        c = self._postings_file.read(1)
        _len_total_tok = ""
        while c != ":":
            _len_total_tok = _len_total_tok + c
            c = self._postings_file.read(1)

        self._total_tokens = int(self._postings_file.read(int(_len_total_tok)))

        # 2. Read total documents
        c = self._postings_file.read(1)
        _len_total_docs = ""
        while c != ":":
            _len_total_docs = _len_total_docs + c
            c = self._postings_file.read(1)

        self._total_docs = int(self._postings_file.read(int(_len_total_docs)))


        # 3. Read corpus into memory
        f = open("%s.corpus" % index_name, "r")
        zstr = f.read()
        f.close()
        self.corpus = cPickle.loads(zlib.decompress(zstr))        

        logging.info("Index loaded!")
        logging.info("Vocabulary size: %d"  % self._total_tokens)
        logging.info("Total indexed documents: %s"  % self._total_docs)


    def query(self, query):
        """
        Perform specified query and return results

            query -- a Query object. See query_engine.Query
        """
        # ----------------------------------
        # Process query groups one at a time
        # ----------------------------------
        results = None
        gid = 0
        scores = [None] * len(query.groups)
        snippets = [None] * len(query.groups)

        for group in query.groups:
            query_terms = group.split(" ")

            for term in query_terms:
                # Fetch postings list
                postings_list = set()
                if term in self._ifile:

                    ## Resolve pointer to postings list (if we have not done so)
                    if not isinstance(self._ifile[term][2], set):
                        self._postings_file.seek(self._ifile[term][2])

                        c = self._postings_file.read(1)
                        list_size = ""
                        while c != ":":
                            list_size = list_size + c
                            c = self._postings_file.read(1)

                        # Read postings list from disk
                        original_postings_list = json.loads("[" + self._postings_file.read(int(list_size)) +"]")
                        postings_list = set()

                        # Porcess the gaps in the postings list
                        last_doc_id = 0
                        i = 0
                        for doc_id in original_postings_list:
                            postings_list.add(doc_id + last_doc_id)
                            last_doc_id = (doc_id + last_doc_id)

                        # Cache postings ist in memory
                        self._ifile[term][2] = postings_list
                    else:
                        # Already resolved, read from memory
                        postings_list = self._ifile[term][2]
                    
                if results is None:
                    results = postings_list
                else:
                    if query.phrase_search:
                        results = results.intersection(postings_list)
                    else:
                        results = results.union(postings_list)

            scores[gid] = dict()
            snippets[gid] = self._compute_score(query_terms, results,
                                            query.phrase_search, scores[gid])

            gid = gid + 1


        # ----------------------------------
        # Aggregate Scores
        # ----------------------------------
        gid = 0
        final_scores = dict()
        final_snippets_positions = dict()
        final_set = dict()
        exclude_set = False
        for group in scores:
            # if gid not in scores:
            #     continue
            for doc in scores[gid]:
                if query.negated_groups[gid]:
                    scores[gid][doc] = (-1.0) * scores[gid][doc]
                elif doc in snippets[gid]:
                    if doc not in final_snippets_positions:
                        final_snippets_positions[doc] = []
                    final_snippets_positions[doc].append(snippets[gid][doc])

                if not doc in final_scores:
                    final_scores[doc] = scores[gid][doc]
                else:
                    final_scores[doc] = final_scores[doc] + scores[gid][doc]
            gid = gid + 1

        i = 0
        for doc in sorted(final_scores, key=lambda d: final_scores[d], reverse=True):
            pos = 0
            if doc in final_snippets_positions:
                pos = min(final_snippets_positions[doc])
            if pos < 0 or pos == sys.maxint:
                pos = 0
            final_set[doc] = [doc, final_scores[doc],pos]
            if i == 0 and final_scores[doc] < 0:
                exclude_set = True
            i = i + 1
            
        if len(final_set) <= 0:
            return []

        # In case first query result is negative, then we
        # will use this set as an "exclusion set")
        filterd_set = final_set
        if exclude_set:
            filterd_set = dict()
            for i in range(self._total_docs):
                if (i + 1) not in final_set:
                    filterd_set[i+1] = [(i+1), 1.0/(i+1), 0]

        # Get top 15 results only
        results = []
        i = 0
        for res in sorted(filterd_set, key=lambda r:filterd_set[r][1], reverse=True):
            results.append([res, filterd_set[res][1], filterd_set[res][2]])
            i = i + 1
            if i >= self.max_results:
                break
        return results
            


    def _compute_score(self, query_terms, results, phrase_search, scores):
        remove_docs = set() # used to exclude docs in phrase searches
        i = 0
        last_term_idx = 0         
        _tmp_scores = dict() 
        snippet_positions = dict()
        for term in query_terms:
            if term not in self._ifile:
                if phrase_search: 
                    return
                else:
                    continue

            term_index = self._ifile[term][0]
            min_proximity = dict()
            for doc in sorted(results):                
                if doc in remove_docs: # this doc has been excluded
                    continue

                if not doc in scores:
                    _tmp_scores[doc] = 0.0

                if not term_index in self._vector_space[doc]:
                    continue

                # Compute IDF for this term
                idf = math.log( self._total_docs / float(self._ifile[term][1]))

                # Compute score
                term_score = idf * self._vector_space[doc][term_index][0]

                # Compute min proximity from previous terms 
                # (From second term onwards) iff it does have previous term
                if i > 0 and last_term_idx in self._vector_space[doc]:
                    if not doc in min_proximity:
                        min_proximity[doc] = sys.maxint

                    # Do it for text and title zones
                    for pos in self._vector_space[doc][term_index][1][Zones.TEXT]:
                        for pos_2 in self._vector_space[doc][last_term_idx][1][Zones.TEXT]:
                            proximity = math.fabs(pos - pos_2)
                            if proximity < min_proximity[doc]:
                                min_proximity[doc] = proximity
                                # This seems like a good snippet position start
                                snippet_positions[doc] = pos_2 - 10

                    for pos in self._vector_space[doc][term_index][1][Zones.TITLE]:
                        for pos_2 in self._vector_space[doc][last_term_idx][1][Zones.TITLE]:
                            proximity = math.fabs(pos - pos_2)
                            if proximity < min_proximity[doc]:
                                min_proximity[doc] = (proximity / 2.0)

                    # Have we found a good snippet position yet?
                    if snippet_positions[doc] == sys.maxint:                        
                        # Doesn't look like it, let's use this one
                        if len(self._vector_space[doc][term_index][1][Zones.TEXT]) > 0:                    
                            snippet_positions[doc] = \
                                self._vector_space[doc][term_index][1][Zones.TEXT][0] - 15

                    # If phrase search and proximity is not 1, exclude this doc
                    if phrase_search and min_proximity[doc] > 1:
                        remove_docs.add(doc)
                else:
                    # This is the first term iteration, we assume our snipper starts here
                    if len(self._vector_space[doc][term_index][1][Zones.TEXT]) > 0:                    
                        snippet_positions[doc] = \
                            self._vector_space[doc][term_index][1][Zones.TEXT][0] - 15
                    else:
                        snippet_positions[doc] = sys.maxint

                _tmp_scores[doc] = _tmp_scores[doc] + term_score
                                   
            # give boost to documents who had small proximities in this round
            if doc in min_proximity:
                _tmp_scores[doc] = _tmp_scores[doc] * (1.0 / min_proximity[doc])

            last_term_idx = term_index
            i = i + 1

        # Add scores up
        for doc in _tmp_scores:
            if doc in remove_docs:
                continue
            else:
                if doc in scores:
                    scores[doc] = scores[doc] + _tmp_scores[doc]
                else:
                    scores[doc] = _tmp_scores[doc]

        return snippet_positions




