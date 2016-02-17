import abc


#Abstract base class for the datagetter
class DataGetterABC(object):

    __metaclass__ = abc.ABCMeta

    """
    DataGetter handles the retrieval of data from the database of addresses.
    Specifically, it retrieves lists of potential matches to an address object
    and can return the frequency of occurence of tokens in addresses 
    """
    
    @abc.abstractmethod
    def get_freq(self,term):
        """
        Given a token, returns the relative frequency with which it appears in the corpus
        For instance, if 'road' appears in one out of a hundred words, would return 0.01
        if passed term = 'road'

        Returns a float.  Returns None if token is not found
        """

    @abc.abstractmethod
    def get_potential_matches_from_address(self, address):
        """
        Given an address object, returns a list of potential matches.

        Returns an dictionary of address objects; the keys are the unique
        ids of the address e.g. a UPRN
        """