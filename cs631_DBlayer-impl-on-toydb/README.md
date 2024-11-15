## DBlayer-on-toydb

This project contains an implementation of a record/tuple layer on top of an existing physical layer and access method layer.

## Team:

* Arif Ali (23m0822)
* Soumik Dutta (23m0826)

## Existing layers:

* **pflayer:** Refer to pf.pdf for details on the physical layer.
* **amlayer:** Refer to am.pdf for details on the access method layer.

### Implemented Tasks

**1. Building a Record Layer (tbl.c and tbl.h)**

* Created a slotted-page structure with page headers storing:
    * Array of pointers to each record within the page.
    * Number of records on the page.
    * Pointer to the free space.
* Implemented functions to create records with variable length types and retrieve attribute values from records. 
* Used a 4-byte `rid` (record ID) for addressing records.

**2. Testing: Loading CSV Data and Indexing ()**

* Worked on loading data from a CSV file into a table using the developed record layer API.
* Filled in sections marked "UNIMPLEMENTED" in `loaddb.c`.
* Implemented the loading process:
    * Read the first line of the CSV file containing schema information.
    * Encode each field based on its data type into record buffers.
    * Insert the record and retrieve its record ID using `Table_Insert`.
    * Create an index entry using `AM_InsertEntry` for the desired field (using the BTree indexer).

**3. Testing: Retrieving Data (dumpdb.c)**

* Implemented data retrieval functionalities in `dumpdb.c`:
    * Handled two retrieval methods based on the command-line argument:
        * Sequential scan using `Table_Scan` ("dumpdb s").
        * Index scan using `AM_` methods to scan the index and then retrieving records using `Table_Get` ("dumpdb i").
    * Decoded retrieved records to print them in the original CSV format for comparison.

### Additional Notes

* Build process required invoking `make` in both `pflayer` and `amlayer` directories before building the `dblayer`.

This work demonstrates a functional record layer interacting with physical and access method layers within the toydb framework. The implemented functionalities allow loading CSV data, creating indexes, and retrieving records efficiently through both sequential and index-based scans.
