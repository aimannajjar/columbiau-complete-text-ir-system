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
import re
import cPickle
import indexer.constants
from collections import defaultdict
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
        self.context = None
        self.max_results = max_results

        # ----------------------------------
        # Load Indexes into memory
        # ----------------------------------
        # Read inverted file
        try:
            f = open("%s.dict" % index_name, "r")
            zstr = f.read()
            f.close()
            self._ifile = cPickle.loads(zlib.decompress(zstr))            
        except Exception, e:
            logging.error("Failed to load index file %s.dict. " \
                          "Please run ./index first" % index_name)
            raise e


        # Read vector space
        try:
            f = open("%s.vs" % index_name, "r")
            zstr = f.read()
            f.close()
            self._vector_space = cPickle.loads(zlib.decompress(zstr))            
        except Exception, e:
            logging.error("Failed to load index file %s.vs. " \
              "Please run ./index first" % index_name)
            raise e



        # Read corpus collection
        try:
            f = open("%s.corpus" % index_name, "r")
            zstr = f.read()
            f.close()
            self.corpus = cPickle.loads(zlib.decompress(zstr))                    
        except Exception, e:
            logging.error("Failed to load index file %s.corpus. " \
              "Please run ./index first" % index_name)
            raise e


        # Read similar context index into memory (for "similar" queries)
        # This requires nltk module present in the system, we do a check 
        # here to prevent runtime crash if module is not available
        try:
            import nltk.text
            f = open("%s.context" % index_name, "r")
            zstr = f.read()
            f.close()
            self.context = cPickle.loads(zlib.decompress(zstr))        
            self.context._word_context_index = \
                            nltk.text.ContextIndex(self.context.tokens,
                                                filter=lambda x:x.isalpha(),
                                                key=lambda s:s.lower())

        except Exception, e:
            logging.warning("You don't have nltk module installed. " \
                            "Similar queries will not be supported")


        # Open handle for postings file -- only read from this file
        # on demand (see fetch_postings() method)
        self._postings_file = open("%s.postings" % index_name, "r")



        # Parse metadata from postings file beginning:
        # Total tokens in index:
        c = self._postings_file.read(1)
        _len_total_tok = ""
        while c != ":":
            _len_total_tok = _len_total_tok + c
            c = self._postings_file.read(1)
        self._total_tokens = int(self._postings_file.read(int(_len_total_tok)))

        # Total documents in index:
        c = self._postings_file.read(1)
        _len_total_docs = ""
        while c != ":":
            _len_total_docs = _len_total_docs + c
            c = self._postings_file.read(1)
        self._total_docs = int(self._postings_file.read(int(_len_total_docs)))


        # DONE
        logging.info("Index loaded!")
        logging.info("Vocabulary size: %d"  % self._total_tokens)
        logging.info("Total indexed documents: %s"  % self._total_docs)


    def query(self, query):
        """
        Perform specified query and return results

            query -- a Query object. See query_engine.Query
        """

        # ----------------------------------
        # Command queries:
        # ----------------------------------
        # "similar" queries:
        if query.cmd == "similar":
            return self.similar_words(query.groups[0])

        # "df" queries:
        elif query.cmd == "df":
            if not query.phrase_search:
                # If it's not a phrase search, then simply return size of postings list
                postings_list = self.fetch_postings(query.groups[0])
                return len(postings_list)
            # For phrase queries, we evaluate the query normally and 
            # return results size (see farther below)

        # "freq" queries
        elif query.cmd == "freq":
            # For non-phrase queries, freq can be obtained easily from 
            # the vector space
            # However, for phrase queries, freq is length of snippets array
            # returned by _compute_group_scores (see farther below)            
            if not query.phrase_search:
                if query.groups[0] in self._ifile:
                    term_index = self._ifile[query.groups[0]][0]

                    # This could become inefficient for large collections
                    # Alternatively, total frequency can be stored somewhere
                    # in the ifile during indexing.
                    total_freq = 0
                    for doc in range(1, len(self._vector_space)):
                        if term_index in self._vector_space[doc]:
                            total_freq = total_freq + \
                                         self._vector_space[doc][term_index][2]
                    return total_freq

                else:
                    return 0                     

        elif query.cmd == "tf":
            # For non-phrase queries, freq can be obtained easily from 
            # the vector space
            # However, for phrase queries, tf is length of snippets array
            # returned by _compute_group_scores (see farther below)      
            if not query.phrase_search:
                if query.groups[0] in self._ifile:
                    term_index = self._ifile[query.groups[0]][0]

                    try:
                        doc_id = int(query.raw_terms[0])
                        if doc_id > 0 and doc_id <= len(self._vector_space) and \
                           term_index in self._vector_space[doc_id]:
                            return self._vector_space[doc_id][term_index][2]
                        else:
                            return 0
                    except Exception, e:
                        return 0

                else:
                    return 0
                    


        # ----------------------------------------
        # Process query groups one at a time
        # And accumulate scores for each document
        # per group

        results = None
        gid = 0
        scores = [None] * len(query.groups)
        snippets = [None] * len(query.groups)


        for group in query.groups:
            query_terms = group.split(" ")

            for term in query_terms:
                # Fetch postings list
                postings_list = self.fetch_postings(term)
                    
                if results is None:
                    results = postings_list
                else:
                    if query.phrase_search:
                        results = results.intersection(postings_list)
                    else:
                        results = results.union(postings_list)

            scores[gid] = dict()
            snippets[gid] = self._compute_group_scores(query_terms, results,
                                            query.phrase_search, scores[gid])

            gid = gid + 1


        # ----------------------------------
        # Aggregate Scores for all groups

        gid = 0
        final_scores = dict()
        final_snippets_positions = dict() 

        # Iterate over groups, add scores up and build a consolidated
        # list of snippet positions for each document 
        for group in scores:
            for doc in scores[gid]:
                if query.negated_groups[gid]:
                    # If a negated group, then use score as a "penalty" instead
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

        # Now that we have the scores, we can start building results set
        # The results set is a map where keys are the document id and
        # values are [doc_id, document_score, snippet_position_start]
        total_positive_scores = 0
        exclude_set = False # If we have negative socres (i.e. when 
                            # processing negated queries), the result
                            # set can be treated as an "exclusion" set
        final_set = dict()
        for doc in sorted(final_scores, key=lambda d: final_scores[d], reverse=True):
            pos = 0
            if doc in final_snippets_positions:
                # we select the first occurance for the snippet :
                pos = min(final_snippets_positions[doc])                                                       

            if pos < 0 or pos == sys.maxint: 
                pos = 0

            final_set[doc] = [doc, final_scores[doc],pos]

            if final_scores[doc] >= 0.0:
                total_positive_scores = total_positive_scores + 1
        
        if total_positive_scores <= 0:
            exclude_set = True    

        # Special Cases
        # - If this is a "df" query for a phrase,
        # we can just return the number of documents with positive 
        # scores here, no need for further processing
        if query.cmd == "df":
            return total_positive_scores

        # - If this is a "freq" query for a phrase,
        # we can just return the number of snippets that contain the
        # phrase
        if query.cmd == "freq":
            total_freq = 0
            for doc in sorted(final_scores, key=lambda d: final_scores[d], reverse=True):
                total_freq += self.phrase_frequency_in_doc(doc, query.groups[0])
            return total_freq

        # - If this is a "tf" query for a phrase,
        # we count the phrases directly, this should not be very
        # expensive operation since we are counting the occurances
        # in one specific document, although it could become inefficient
        # for very large documents
        if query.cmd == "tf":
            doc_id = 0
            try:
                doc_id = int(query.raw_terms[0])
            except Exception, e:
                return 0

            return self.phrase_frequency_in_doc(doc_id, query.groups[0])        

        if len(final_set) <= 0:
            return []


        # Filtered set is ALL_DOCUMENTS - EXCLUSION_SET 
        filterd_set = final_set
        if exclude_set:
            filterd_set = dict()
            for i in range(self._total_docs):
                if (i + 1) not in final_set:
                    filterd_set[i+1] = [(i+1), 1.0/(i+1), 0]


        # Finally, let's get back max_results only out of the entire results
        results = []
        i = 0
        for res in sorted(filterd_set, key=lambda r:filterd_set[r][1], reverse=True):
            results.append([res, filterd_set[res][1], filterd_set[res][2]])
            i = i + 1
            if i >= self.max_results:
                break

        # Whoa, done.
        return results

    def fetch_postings(self, term):
        """Resolve postings from disk and cache it in memory"""
        postings_list = set()
        if term in self._ifile:

            # Resolve pointer to postings list (if we have not done so before)
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
        return postings_list

    def similar_words(self, word,n=5):
        """Find all terms similar in context to the specified word"""
        if not self.context:
            logging.warning("You don't have nltk module installed. " \
                            "Similar queries will not be supported")
            return []            

        # This mirrors the method implemented in nltk module with 
        # the exception that it does not print out any output but instead
        # return the results
        scores = defaultdict(int)
        for c in self.context._word_context_index._word_to_contexts[self.context._word_context_index._key(word)]:
            for w in self.context._word_context_index._context_to_words[c]:
                if w != word:
                    scores[w] += \
                        self.context._word_context_index._context_to_words[c][word] \
                        * self.context._word_context_index._context_to_words[c][w]

        return sorted(scores, key=scores.get)[:n]


    def phrase_frequency_in_doc(self, doc_id, phrase):
        """Compute the total frequency of a phrase in a specific index"""
        if doc_id > 0 and doc_id < len(self.corpus):
            regex = ""
            for term in re.compile(indexer.constants.DELIMITERS).split(phrase):                
                regex = regex + re.escape(term) + indexer.constants.DELIMITERS

            return len(re.findall(regex, self.corpus[doc_id].original_text)) + \
                   len(re.findall(regex, self.corpus[doc_id].title)) + \
                   len(re.findall(regex, self.corpus[doc_id].author))
        else:
            return 0



    ## Private Methods ##
    def _compute_group_scores(self, query_terms, documents, phrase_search,
                              scores):
        """
        Evaluate scores for specified query terms for all specified 
        documents and update the passed scores array. Return empty list
        when query_terms are not within a proximity window of size 1
        """
        # Compute scores for all documents in "documents" set for query
        # terms in query_terms and return list of snippets positions which
        # contain the terms.
        # Reminder: "documents" here is
        #  * The intersection of postings lists for terms in a phrase OR
        #  * The union of postings lists for terms in non-phrase search

        non_phrase_docs = [] # in phrase-search, we append documents
                              # that do not contain the query terms
                              # in a single phrase in this list

        i = 0        
        prev_term_idx = 0         
        prev_term = None
        group_scores = {}
        snippet_positions = {}

        for term in query_terms:

            if term not in self._ifile:
                if phrase_search: 
                    return []
                else:
                    continue

            term_index = self._ifile[term][0]
            min_proximity = dict() # A map where key is doc and value is 
                                   # smallest proximity between this term
                                   # and previous one


            for doc in sorted(documents):                

                if not doc in scores:
                    group_scores[doc] = 0.0

                if not term_index in self._vector_space[doc]:
                    continue

                # Compute IDF for this term
                idf = math.log( self._total_docs / float(self._ifile[term][1]))

                # Compute score
                term_score = idf * self._vector_space[doc][term_index][0]

                # --------------------------------------------            
                # Compute proximity from previous term
                # --------------------------------------------
                # Applicable in second-iteration and onward
                if i > 0 and prev_term_idx in self._vector_space[doc]:
                    if not doc in min_proximity:
                        min_proximity[doc] = sys.maxint

                    # Since the term may appear in several positions, we
                    # compute proximities between all positions for both terms
                    # and we use the smallest value as a representative 
                    # of proximity. As a side-effect, when proximity is smallest
                    # we use the word position as a starting position for result
                    # snippet

                    # For Text zone:
                    for pos in self._vector_space[doc][term_index][1][Zones.TEXT]:
                        for pos_2 in self._vector_space[doc][prev_term_idx][1][Zones.TEXT]:
                            proximity = pos - pos_2

                            if not phrase_search and proximity < 0:
                                # For non-phrase, order is not important
                                # we can take the absoulte value
                                proximity = proximity * (-1)
                            
                            if proximity > 0 and proximity < min_proximity[doc]:
                                min_proximity[doc] = proximity
                                snippet_positions[doc] = pos_2

                    # For title zone:
                    for pos in self._vector_space[doc][term_index][1][Zones.TITLE]:
                        for pos_2 in self._vector_space[doc][prev_term_idx][1][Zones.TITLE]:
                            proximity = pos - pos_2

                            if not phrase_search and proximity < 0:
                                # For non-phrase, order is not important
                                # we can take the absoulte value
                                proximity = proximity * (-1)

                            if proximity > 0 and proximity < min_proximity[doc]:
                                min_proximity[doc] = proximity



                    # Have we found a good snippet position for this document yet?
                    if snippet_positions[doc] == sys.maxint:                        
                        # Doesn't look like it, let's use this one
                        if len(self._vector_space[doc][term_index][1][Zones.TEXT]) > 0:                    
                            snippet_positions[doc] = \
                                self._vector_space[doc][term_index][1][Zones.TEXT][0]

                    # If phrase search and proximity is not 1, then return empty
                    if phrase_search and min_proximity[doc] != 1:
                        non_phrase_docs.append(doc)              
                        
                else:
                    # This is the first term iteration, we assume our snippet starts here
                    if len(self._vector_space[doc][term_index][1][Zones.TEXT]) > 0:                    
                        snippet_positions[doc] = \
                            self._vector_space[doc][term_index][1][Zones.TEXT][0]
                    else:
                        snippet_positions[doc] = sys.maxint

                group_scores[doc] = group_scores[doc] + term_score
                                   
                # use proximity as scoring factor (the smaller the better)
                if doc in min_proximity:
                    group_scores[doc] = group_scores[doc] * (1.0 / min_proximity[doc])

            prev_term_idx = term_index
            prev_term = term
            i = i + 1

        # Update the scores array 
        for doc in group_scores:
            if phrase_search and doc in non_phrase_docs:
                del snippet_positions[doc]
                continue

            if doc not in scores:
                scores[doc] = 0.0
            scores[doc] = scores[doc] + group_scores[doc]

        return snippet_positions

