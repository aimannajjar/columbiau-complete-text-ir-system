# Complete Basic IR System
* COMS 6998-6 Search Engine Technology (Fall 2012)
* Columbia University
* Done by:
  * Aiman Najjar (an2434)


1 . Installation & Usage:
--------------------------
Unpack archive and from the command line in a Linux machine run using Python 2.7.3+

To index a collection of documents, run:

./index /path/to/collection-directory


To start the interactive query console, run:

./query

A sample collection of documents is provided in the package in ./cranfieldDocs

Query syntax: The following query example set cover all supported query types:


  cat             - returns any document that has the word "cat" in it
  cat dog         - any document that has one or more of these words
               ("fuzzy or" is assumed by default)
  cat dog rat     - up to 10 words in a query
  "tabby cat"     - phrases of up to 5 words in length
  "small tabby cat" "shaggy dog" : multiple phrases in a query
  !cat
  !"tabby cat"    - negations of single words or phrases
  !cat !dog       - multiple negations per query
  "similar cat"	: returns all the  words which are similar in context
  df "pack rat"   - shows the number of documents in which "pack
                    rat" appears in the index
  df rat          - shows the number of documents in which rat
                    appears in the index
  freq "pack rat" - shows how many times the phrase "pack rat"
                    appears in the index
  doc 4562        - shows the full text of document 4562 (ideally
                    through a pager)
  tf 4562 rat     - shows the term frequency of rat in document 4562
  title 4562      - show the title of document 4562


2. Documents Structure
--------------------------
Documents must be structured in the following XML format:


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


3. Disclosures:
--------------------------
This IR system makes use of the following libraries, all are included in the package:
* PorterStemmer implementation in Python: http://www.tartarus.org/~martin/PorterStemmer
* NLTK (Optional): http://http://nltk.googlecode.com/
* YAML: Required by NLTK

NLTK is only used to answer "similar" queries, it's not required to be present for other queries. However, as stated above all libraries are included in this package and there is no need to perform any extra installation steps


4. System Architecture
---------------------------------------
The main components of the system are:
* Indexer: Provides an API to index documents
* QueryEngine: Provides an API to load index from disk and execute queries, also provides an interface to parse query strings
* Document: Provides an API to parse documents and encapsulate them in runtime objects

In addition to the IR framework, the following scripts are provided as an example usage:
* index.py: Given a path to documents collection, the script builds an index and save it to disk using the name ifile.*, an executable shell wrapper is available with the name "index"
* query.py: Loads the index named "ifile" and invokes an interactive interface to answer queries. Index files are assumed to be present in current working directory. A shell wrapper is available with the name "query"

4.1 Index Structure
---------------------------------------

Upon a successful completion of an indexing run, the following files will be saved in the disk:
* INDEX_NAME.ifile: contains a partial inverted file of the collection. Pointer to postings lists positions are stored instead of the posting list, this is useful at query time since postings lists for terms will not loaded in memory unless needed
* INDEX_NAME.vs: A vector space representation for the entire corpus
* INDEX_NAME.postings: Postings lists encoded in D-Gap fashion.
* INDEX_NAME.corpus: The entire original corpus is stored here, helpful to render output (e.g. snippets) 
* INDEX_NAME.context: This is used by NTLK to perform "similar" queries


6. API Documentation
---------------------------------------
The API is documented using Python docstrings, I've attempted to adhere as much as possible to PEP-257 conventions. The documentation can be viewed offline at docs/index.html or using pydoc tool.


7. Additional Features
---------------------------------------
The main weighing scheme I used to determine documents ranking is TF-IDF, however, in addition to the standard TF-IDF the following factors greatly contribute to documents ranking:
* Proximity based ranking: Documents in which terms occur within smaller proximity windows are given higher scores
* Score are normalized by document length to ensure that there is no bias toward long documents
* Terms weights are multiplied by Inverse Document Frequency (IDF) so that popular terms across the collection are given less weight
* Zone Indexing: Document in which a query term(s) appear in Title are ranked higher, my reasoning is that normally a document title is an excellent representative of the document topic (e.g. it's more likely that a document is discussing "databases" when the term "database" appear in the title), similarly, more weight is given to Author metadata -- weight factors can be changed in document.document.Zones 

Also other additional features include: 
* Multi-threaded indexing
* A vector-space representation is constructed during indexing although not directly exposed via API.
* The interactive query script highlight returned result set

8. Further Work
---------------------------------------
I have attempted to implement many ideas in the IR literature to improve ranking and scoring as well as for faster query time processing, however, I believe there is room for optimization in the indexer to improve index compression and reduce the index size. Currently, I'm compressing the index size on disk using ZLIB, I've implemented one compression technique for the postings lists which is encodig the list using D-Gap method, I still believe more can be done in that area in particular. 


