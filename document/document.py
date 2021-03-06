#!/usr/bin/env python
# file: indexer/document.py
# author: Aiman Najjar (an2434), Columbia University

"""
This module provides an interface to read documents and populate
an object of type Document. It also defines the Document class
which is a class representation of the corpus.
"""
import HTMLParser
import logging
import re
import PorterStemmer

class Zones:
    """Defines constants for index zones"""
    AUTHOR = 0
    TITLE = 1
    BIBLIO = 2
    TEXT = 3

    WEIGHTS = [0,0,0,0]
    WEIGHTS[AUTHOR] = 3.0    
    WEIGHTS[TITLE] = 2.0
    WEIGHTS[BIBLIO] = 1.2
    WEIGHTS[TEXT] = 1.0

class Document(HTMLParser.HTMLParser):
    """
    Simple object that stores a document text and metadata, defines
    the following attributes per document:
        document_id -- a numberical value that can be manually
                        set by a client of this object
        document_number -- the numerical value found in <DOCNO> tag
        author, biblio, text -- document metadata and content
        weighted_length -- total terms in document, terms from different
                            zones contribute differently to this weighted 
                            length (e.g. a single term in the title is
                                worth two terms)

    There are two ways to create a Document object:
        from_file(path_to_xml_file) -- Creates a document by passing
                                       a path to the xml file
        from_xml(xml_data) -- Creates a document by directly passing
                              the XML data

    The expected XML format is:
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
    """

    @classmethod
    def from_file(cls, path_to_file):
        """Read specified file and return a Document object"""
        f = open(path_to_file, 'r')
        file_contents = f.read()
        f.close()
        document = Document.from_xml(file_contents)
        if document is None:
            return None

        document._path_to_file = path_to_file
        return document

    @classmethod
    def from_xml(cls, xml):
        """
        Parse the xml data and return a Document object.
            xml -- an xml document in the following format:
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
        """
        document = Document()
        try:
            xml = xml.decode('UTF-8')
        except UnicodeDecodeError, e:
            xml = xml
        document.feed(xml)

        if document.document_number is None or document.author is None \
                or document.biblio is None or document.text is None \
                or document.title is None:
            return None

        return document


    def __init__(self):
        self.document_id = 0 
        self.document_number = None
        self.author = None
        self.biblio = None
        self.text = None
        self.original_text = None
        self.title = None
        self.weighted_length = 0 
        self._path_to_file = None

        # variables used during parsing
        self.reset()
        self.data = ""
        self.original_data = ""
        self.currentTag = ""

    def colored_author(self, terms):
        """Return the author field with highlighted terms """
        author = self.author
        for term in terms:
            author = re.sub('(?i)' + re.escape(term),
                           '\033[94m' + term + '\033[0m', author)

        return author


    def colored_title(self, terms):
        """Return the title with highlighted terms """
        title = self.title
        for term in terms:
            title = re.sub('(?i)([\s.,=?!:@<>()\"-;\'&_\\{\\}\\|\\[\\]\\\\]' + \
                             re.escape(term) + \
                             "[^\s.,=?!:@<>()\"-;\'&_\\{\\}\\|\\[\\]\\\\]*)",
                             '\033[94m\\1\033[0m', title) 

        return title

    def text_snippet(self, terms, start, length):
        """
        Return a snippet from pos start to end with highlighted terms
            start - the "word" position (as opposed to characater position)
            length - how many words to include


        """

        start_found = False
        new_start = 0
        new_end = 0
        pos = start

        for term in self.text.split(" "):
            pos = pos - 1

            if not start_found:
                new_start = new_start + 1
            else: 
                new_end = new_end + 1

            if not start_found and pos <= 0:
                pos = length
                start_found = True
            elif pos <= 0:
                break
        new_end = new_start + new_end
        snippet = " ".join(self.text.split(" ")[new_start:new_end])

        for term in terms:
            p = PorterStemmer.PorterStemmer()
            term = p.stem(term, 0,len(term)-1)
            snippet = re.sub('(?i)([\s.,=?!:@<>()\"-;\'&_\\{\\}\\|\\[\\]\\\\]' + \
                             re.escape(term) + \
                             "[^\s.,=?!:@<>()\"-;\'&_\\{\\}\\|\\[\\]\\\\]*)",
                             '\033[94m\\1\033[0m', snippet) 

        return snippet

    def handle_starttag(self, tag, attrs):
        self.currentTag = tag

    def handle_endtag(self, tag):
        zone = Zones.TEXT
        if tag.lower() == "docno":
            self.document_number = self.data
        elif tag.lower() == "author":
            self.author = self.data
            zone = Zones.AUTHOR
        elif tag.lower() == "biblio":
            self.biblio = self.data
            zone = Zones.BIBLIO
        elif tag.lower() == "text":
            self.text = self.data
            self.original_text = self.original_data
            zone = Zones.TEXT
        elif tag.lower() == "title":
            self.title = self.data
            zone = Zones.TITLE
        elif tag.lower() != "doc":
            logging.warning("Unexpected tag %s" % tag)

        self.weighted_length = self.weighted_length + \
                                (len(self.data) * Zones.WEIGHTS[zone])
        self.currentTag = ""
        self.data = ""
        self.original_data = ""

    def handle_data(self, d):
        self.data = d.replace("\n", " ")
        self.original_data = d


