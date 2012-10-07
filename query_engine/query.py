#!/usr/bin/env python
# file: indexer/document.py
# author: Aiman Najjar (an2434), Columbia University

"""
This module provides an interface to parse a query string and populate
the query object
"""
import re
import indexer.constants 
from document.PorterStemmer import PorterStemmer

class Query():
    """
    Example queries:
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
    """

    def __init__(self):
        """Initialize query"""
        self.groups = []
        self.negated_groups = dict()
        self.phrase_search = False

    @staticmethod
    def from_string(query): 
        """Parse specified query and return query object"""
        # Is it a phrase search? currently the engine
        # supports one type of search in a single query (single-word or phrase)
        queryObj = Query()
        queryObj.phrase_search = False

        # Clean up and determine if phrase search
        query = query.strip()
        if query.replace("!", "").startswith('"'):
            queryObj.phrase_search = True
        
        last_grp = None

        gid = 0
        _groups = []

        # Populate groups
        if not queryObj.phrase_search:
            for group in query.split(" "):
                if group.strip().startswith("!"):
                    _groups.append(group.strip()[1:])
                    queryObj.negated_groups[gid] = True
                    gid = gid + 1
                else:
                    _groups.append(group.strip())
                    queryObj.negated_groups[gid] = False
                    gid = gid + 1
        else:
            for group in query.split('"'):
                if group.strip(' "') == '': 
                    continue
                if group.strip(' "') == '!': 
                    last_grp = group
                    continue

                if last_grp is not None and "!" in last_grp:
                    _groups.append(group)
                    queryObj.negated_groups[gid] = True
                    gid = gid + 1
                else:
                    _groups.append(group)
                    queryObj.negated_groups[gid] = False
                    gid = gid + 1
                last_grp = group

        # Stem & remove inelgible tokens
        for group in _groups:
            _query_terms = group.split(" ")
            query_terms = []
            for term in _query_terms:
                if term not in indexer.constants.DO_NOT_INDEX:
                    # Stem
                    p = PorterStemmer()
                    term = term.lower()
                    term = p.stem(term, 0,len(term)-1)
                    query_terms.append(term)
            queryObj.groups.append(' '.join(query_terms))


        return queryObj


        