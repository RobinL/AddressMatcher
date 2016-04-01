import abc





import psycopg2
from data_getters.data_getter_abc import DataGetterABC
from address_matcher.other_functions import memoize
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
import pandas as pd
from pandas.io.sql import DatabaseError
import random
from address_matcher import Address

class DataGetter_Memory(DataGetterABC):

    def __init__(self, match_targets, freq_df):
        self.potential_matches = [Address(this_one) for this_one in list(match_targets)]
        self.freq_df = freq_df


    def get_freq(self,term):
        """
        Given a token, returns the relative frequency with which it appears in the corpus
        For instance, if 'road' appears in one out of a hundred words, would return 0.01
        if passed term = 'road'

        Returns a float.  Returns None if token is not found
        """
        try:
            return self.freq_df[self.freq_df["term"]==term]["freq"].iloc[0]
        except:
            return None

    def get_potential_matches_from_address(self, address):
        """
        Given an address object, returns a list of potential matches.

        Returns an dictionary of address objects; the keys are the unique
        ids of the address e.g. a UPRN
        """

        return self.potential_matches