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

    Another type of queries is the "command"-based query:
    df "pack rat"     - shows the number of documents in which "pack
                       rat" appears in the index
    df rat            - shows the number of documents in which rat
                       appears in the index
    freq "pack rat"   - shows how many times the phrase "pack rat"
                       appears in the index
    doc 4562          - shows the full text of document 4562 (ideally
                       through a pager)
    tf 4562 rat       - shows the term frequency of rat in document 4562
    title 4562        - show the title of document 4562    

    To initialize a query from a string, call Query.from_string()

    The parsed query object has the following attributes:
        groups - each group represents a disjunct in the disjunctive-
                 normal-form of the query
        negated_groups - a map of flags indicate whether the group is 
                         negated (the key is group index)
        phrase_search - true/false flag, currently a query can be either
                        a phrase search or not
        cmd - if this is set, then it can be one of the following values:
                "similar", "df", "freq", "doc", "tf", "title"
        raw_terms - a list of all query terms in the query before processing
                    excluding stop-words

    """

    def __init__(self):
        """Initialize query"""
        self.groups = []
        self.raw_terms = []
        self.negated_groups = dict()
        self.phrase_search = False
        self.cmd = None 

    @staticmethod
    def from_string(query): 
        """Parse specified query and return query object"""
        queryObj = Query()
        queryObj.phrase_search = False        
        query = query.strip().lower()


        # Determine if it's a "command" query
        if (query.startswith("similar ") or query.startswith("df ") or \
            query.startswith("freq ") or query.startswith("doc ") or \
            query.startswith("tf ") or query.startswith("title ")) and \
            len(query.split(" ")) > 1:
                queryObj.cmd = query.split(" ")[0].strip()
                query = query.replace(queryObj.cmd + " ", "", 1) # remove cmd
                                                              # from query str

        # For "tf " queries, extract first parameter early on, so we
        # don't have to hack later when we process the query terms
        if queryObj.cmd == "tf":
            if len(query.split(" ")) < 2: 
                # This is not a valid "tf " query
                queryObj.cmd = None
            else:
                queryObj.raw_terms.append(query.split(" ")[0])
                query = " ".join(query.split(" ")[1:])

        # Clean up and determine if phrase search        
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

        # Stem tokens in groups (except for "similar" queries) 
        # and remove inelgible tokens
        for group in _groups:

            _query_terms = []
            if queryObj.cmd == "doc" or queryObj.cmd == "title":
                _query_terms = group.split(" ")

            else:
                _query_terms = re.compile(indexer.constants.DELIMITERS).split(group)

            query_terms = []
            for term in _query_terms:
                term = term.lower()

                if term not in indexer.constants.DO_NOT_INDEX:
                    queryObj.raw_terms.append(term)
                    # Stem
                    if queryObj.cmd != "similar":
                        p = PorterStemmer()
                        term = term.lower()
                        term = p.stem(term, 0,len(term)-1)
                    query_terms.append(term)
            queryObj.groups.append(' '.join(query_terms))


        return queryObj


        