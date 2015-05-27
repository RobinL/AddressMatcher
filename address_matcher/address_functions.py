 #!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import os

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def fix_postcode_in_string(address):
    """
    If there is a postcode in the string, make sure it's upper case and has a space
    """

    address = address.upper()
    pc = get_postcode(address)

    if pc:
        pc_s = postcode_split(pc)
        address = address.replace(pc, pc_s)

    return address

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
            return postcode[:-3] + " " + postcode[-3:]
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