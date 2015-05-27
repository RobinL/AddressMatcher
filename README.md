Package for fuzzy matching addresses in different datasets.

To conduct address matching, three main objects are used:

* An address object, for individual addresses.  This handles parsing, tokenising, standardising the text etc.
* A matcher object, which contains the core matching algorithm.  Given an "address_to_match", and a dict of potential_matches, it will find the best match
* A datagetter object.  This has two functions:  first, given an address, it can get_potential_matches_from_address.  Second, given a token, it returns the frequency with which that token is seen in the corpus of all addresses.  This helps the algorithm places greater weight on tokens which have higher 'discriminativeness'.

Functions quite effectively but still things to do:

* Need unit tests
* Need to clarify treatment of unicode.  Probably convert all strings to unicode at earliest opportunity.  
* Need to be more explicit about use of arbitrary constants
