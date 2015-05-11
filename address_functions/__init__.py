 #!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import os

from collections import OrderedDict

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def get_postcode(address_string):
    """
    Takes an address and returns the postcode, or None if no postcode is found.
    """

    address_string = address_string.upper()
    pc_regex = "([A-PR-UWYZ]([1-9]([0-9]|[A-HJKSTUW])?|[A-HK-Y][1-9]([0-9]|[ABEHMNPRVWXY])?) *[0-9][ABD-HJLNP-UW-Z]{2}|GIR *0AA)"
    matches = re.search(pc_regex, address_string)

    if matches:
        return matches.group(1)
    else:
        return None

def postcode_split(postcode):
    """
    If the postcode has no space in the middle add one 
    """
    if postcode:
        if " " not in postcode.strip():
            return postcode[:-3] + postcode[-3:]
        else:
            return postcode.strip()
    else:
        return postcode


def get_numbers(address_string_no_postcode):
    """
    Retrieves a list of all the numbers in an address that are not part of the postcode
    """
    num_list = re.findall("\d+", address_string_no_postcode)
    return num_list


import string
def get_tokens(address_string):
    """
    Tokenizes an address into its words
    """

    exclude = set(".")
    address_string = ''.join(ch for ch in address_string if ch not in exclude)
    address_string = ''.join(ch for ch in address_string if ch not in exclude)

    #Look for things in the address that look like "A B C Catering" and convert to "ABC Catering" to prevent common tokens of single letters
    if re.search(r"(\b|^)(\w) (\w) ", address_string):
        for i in range(10,1,-1):
            replace_string = "".join(["\\"+str(j) for j in range(2,i+2)]) + " "
            regex_string = r"(\b|^)"+"(\w) "*i
            address_string = re.sub(regex_string,replace_string, address_string)

    word_list = re.sub("[^\w]", " ",  address_string).split()
    word_list = [w.upper() for w in word_list]

    exclude = ("LTD", "LIMITED", "AND") #These harm distinguishability because the overall probability is low, but amongst companies, it's very high

    word_list = [w for w in word_list if w not in exclude]

    word_list = list(OrderedDict.fromkeys(word_list))
    return word_list



