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


    def __init__(self,):
        self.document_id = 0 
        self.document_number = None
        self.author = None
        self.biblio = None
        self.text = None
        self.title = None
        self.length = 0
        self._path_to_file = None

        # variables used during parsing
        self.reset()
        self.terms_list = []
        self.currentTag = ""

    def handle_starttag(self, tag, attrs):
        self.currentTag = tag

    def handle_endtag(self, tag):
        data = ''.join(self.terms_list).strip()
        zone = Zones.TEXT
        if tag.lower() == "docno":
            self.document_number = data
        elif tag.lower() == "author":
            self.author = data
            zone = Zones.AUTHOR
        elif tag.lower() == "biblio":
            self.biblio = data
            zone = Zones.BIBLIO
        elif tag.lower() == "text":
            self.text = data
            zone = Zones.TEXT
        elif tag.lower() == "title":
            self.title = data
            zone = Zones.TITLE
        elif tag.lower() != "doc":
            logging.warning("Unexpected tag %s" % tag)

        self.length = self.length + (len(data) * Zones.WEIGHTS[zone])
        self.currentTag = ""
        self.terms_list = []

    def handle_data(self, d):
        self.terms_list.append(d.replace("\n", ""))


