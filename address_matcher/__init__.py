 #!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import division

import re
import math
import Levenshtein
import address_functions as af
import logging
import string
from collections import OrderedDict

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class Address(object):

    """
    The address object holds addresses, whether they be the target address (the address for which we are trying
    to find a match)or the potential matches (the list of addresses which may match the target)
    """

    def __init__(self,full_address, data_getter=None): #If it's given a freq_conn it will use it to order tokens

        self.full_address = None
        self.full_address_no_postcode = None
        self._postcode = None
        self.numbers = None

        self.id = None
        self.probability = 1
        self.geom_wkt = None

        self.data_getter = data_getter

        self.tokens_original_order_postcode = None
        self.tokens_original_order_no_postcode = None
        self.tokens_specific_to_general_by_freq = None

        if type(full_address)== str:  #This is Python 2, so if we're dealing with bytes, encode to utf-8
            full_address = full_address.decode("utf-8")

        #get rid of any double spaces
        full_address =  re.sub("\s{2,100}", " ", full_address)
        
        #Standardise text and postcode
        full_address = full_address.upper()
        full_address = af.fix_postcode_in_string(full_address)
        self.full_address = full_address
        
        #If we assign a full address, automatically parse the postcode (note this can be overridden
        #later if we know the postcode)
        pc = af.get_postcode(full_address)
        pc = af.postcode_split(pc)
        self.postcode = pc

        if self.postcode:
            self.full_address_no_postcode = full_address.replace(af.get_postcode(full_address),"")
        else:
            self.full_address_no_postcode = full_address


        #Get rid of trailing comma and space from address once postcode had been removed
        self.full_address_no_postcode =  re.sub(", $", "", self.full_address_no_postcode)
        
        #Any numbers which are in the address will be treated slightly differently - extract them from the address
        self.numbers = af.get_numbers(self.full_address_no_postcode)

        self.tokens_no_postcode = self.tokenise(self.full_address_no_postcode)
        self.tokens_postcode = self.tokenise(full_address)
        self.set_tokens_original_order_postcode()
        self.set_ordered_tokens_freq()

        self.match_score = 0
        self.relative_score = 0
        self.match_description = ""
        

    @property
    def postcode(self):
        return self._postcode

    @postcode.setter
    def postcode(self,value):
        """
        If we explicitly assign a postcode, then update the full address no postcode
        This overrides the regex 
        """

        self._postcode = af.postcode_split(value)

        if af.postcode_split(value):
            self.full_address_no_postcode = self.full_address.replace(value,"")
            #Get rid of trailing comma and space
            self.full_address_no_postcode =  re.sub(", $", "", self.full_address_no_postcode)
        else:
            self.full_address_no_postcode = self.full_address


        self.numbers = af.get_numbers(self.full_address_no_postcode)


    def set_tokens_original_order_postcode(self):
        """
        Get the individual words which make up the address, excluding the postcode
        This will be used to run full text searches on the address.

        The original ordering of the tokens may be useful -
        But note it's neither general to specific or specific to general.  Although the text of an address
        tends to be specific to general, the postcode breaks this rule.

        """

        self.tokens_original_order_no_postcode= self.tokens_no_postcode
        self.tokens_original_order_postcode = self.tokens_postcode

    def set_ordered_tokens_freq(self):

        """
        Order the tokens by their term frequency using the connection
        """

        if self.data_getter:
            tokens_to_score = list(set(self.tokens_postcode))
            scored_tokens = [{"token" : t, "score": self.data_getter.get_freq(t)} for t in tokens_to_score]
            scored_tokens.sort(key=lambda x: x["score"])
            scored_tokens = [t["token"] for t in scored_tokens if t["score"]] #Only keep tokens if they exist in abp!!
            self.tokens_specific_to_general_by_freq = scored_tokens


    def tokenise(self,address_string):
        """
        Tokenizes an address into its words
        """

        exclude = set(".'")
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

        return word_list

    def set_match_stats(self, target_address):
        pass



    def __repr__(self):

        if self.probability:
            if hasattr(self,"match_description"):
                return u"Uprn: {0}.  Address: {1}.  Probabilty: {2:.2g}. Score: {3:.2f} {4}. Relative score: {5:.2f}".format(self.id, self.full_address, self.probability, self.match_score, self.match_description, self.relative_score)

        if self.probability:
            return u"Uprn: {0}.  Address: {1}.  Probabilty {2:.2g}".format(self.id, self.full_address, self.probability)
        return u"{}".format(self.full_address)

class Matcher(object):

    """
    The matcher stores the target address and the list of potential matches
    and contains the core algorithms that perform the match and compute
    scores on the match

    Note that it doesn't deal with obtaining the list of potential matches.  
    This is delegated to a 'data_getter' class - which will be specific
    to the application.
    """

    def __init__(self, data_getter ,address_obj):
        """
        address_obj is the 'target address' - the one for which we want to find a match
        data_getter is a database connection to the AddressBase Premium dataset
        """

        self.data_getter = data_getter
        self.address_to_match = address_obj

        self.potential_matches = [] #This is a list of address objects.  
        
        self.distinguishability = None
        self.found = False


        self.one_match_only = False

        self.fuzzy_matched_one_number = False 

        self.best_match = Address("")


    def load_potential_matches(self):
        """
        Add list of potenital matches to the matcher object.
        If only a single match is found 'short cut' the code to make it the best match
        """
        pot_matches = self.data_getter.get_potential_matches_from_address(self.address_to_match)
        self.potential_matches.extend(pot_matches)
        #If there's a single match, assign it to the best match
        if len(self.potential_matches) ==1:
            self.one_match_only = True
        else:
            self.one_match_only = False


    def set_prob_on_address(self, address):

        """
        Copmute a score than in some limited sense represents the inverse likelihood
        that this address and the target address match. 

        Note this is not a true probability - but the smaller the probability, the 
        better the match
        """
        

        def is_number(token):
            """
            Checks whether the token is a number 

            The code treats house numbers (including numbers like 10A) slightly differently
            to other tokens.  This is needed so that if an exact match is not found, the  
            closest match will be a property next door (or as close as possible)
            the the target address
            """
            matches = re.search("^(\d+)[ABCDEFG]?$",token)
            if matches:
                return True
            else:
                return False

        def get_number(token):
            """
            Retrieves the number
            
            The code treats house numbers (including numbers like 10A) slightly differently
            to other tokens.  This is needed so that if an exact match is not found, the  
            closest match will be a property next door (or as close as possible)
            the the target address
            """
            matches = re.search("^(\d+)[ABCDEFG]?$",token)
            return int(matches.group(1))


        def levenshtein_ratio(str1, str2):
            """
            Computes a ratio of number of edits to length of string
            """
            str1 = unicode(str1)
            str2 = unicode(str2)
            d = Levenshtein.distance(str1,str2)
            length = max(len(str1), len(str2))
            return 1 - (d/length)

        def get_prob(potenital_token):
            """
            Computes a score that represents the 'distinctness' or 'discriminativeness' of this token
            - i.e. how much it helps in narrowing down the full list of all addresses.

            A score of 1 means that this token doesn't narrow down the full list at all.  A score of 0.01
            means it cuts it down by 99 per cent etc.

            Note this is not a true probability because if the token cannot be found in the target address, 
            the code attempst to find a fuzzy match.  The probability here is ill defined.

            Note that the eventual score will not be a true probability because it does not take into account any correlations
            between term frequencies (i.e. it is similar to naive bayes).
            """


            main_prob = self.data_getter.get_freq(potenital_token)


            #If this potential match token matches one of the tokens in the target address, then compute how
            #unusual this token is amongst potential addresses.  The more unusual the better
            if potenital_token in self.address_to_match.tokens_original_order_postcode:

                return_value = main_prob

                #logger.debug("potential token: {} found {}".format(potenital_token,return_value))
                return return_value

            #if this token from one of the potetial matches is not in the target address, then maybe there's a spelling error?
            #Compare this token to each token in the target address looking for similarities

            best_score = 1

            for target_token in self.address_to_match.tokens_original_order_postcode:

                prob = self.data_getter.get_freq(target_token)

                if prob == None: #If the prob is None that means we couldn't find it
                    prob = 3.0e-7


                if is_number(target_token) and is_number(potenital_token) and self.fuzzy_matched_one_number == False:



                    #We will want to check whether the tokens are 'number like' - if so, then 125b matches 125 and vice versa, but
                    #225 does not match 125 closely.  125 however, is a reasonable match for 126.
                    t_num = get_number(target_token)
                    p_num = get_number(potenital_token)


                    #Calculate a distance metric using arbitrary constants such as 5 and 2.  Monotonic in closeness to actual number

                    d_num1 = t_num + 5
                    d_num2 = p_num + 5

                    #how far away is potential from target?
                    distance = math.fabs(d_num1-d_num2)/(max(d_num1,d_num2))
                    if distance != 0:
                        distance += 0.2

                    #logger.debug("t_num = {}, p_num = {}, distance = {}, main_prob {}".format(t_num, p_num, distance, prob))

                    #logger.debug("adjust up by {}".format(((distance+1)**4)))
                    prob = prob *((distance+1)**4)*10

                    #logger.debug("using prob {}".format(prob))

                    if prob < 1:
                        self.fuzzy_matched_one_number = True

                    best_score = min(best_score, prob)

                elif not is_number(target_token) and not is_number(potenital_token):

                    #proceed to fuzzy match only if both tokens are >3 characters, otherwise best score remains 1
                    if len(target_token)> 3 and len(potenital_token)>3:
                        l_ratio = levenshtein_ratio(target_token, potenital_token)

                        #If the ratio is better than 0.7 assume it's a spelling error
                        if l_ratio>0.5:


                            prob = self.data_getter.get_freq(potenital_token) #DOES THIS MAKE SENSE TO USE POTENTIAL TOKEN HERE?
                            if prob is None:
                                prob = 1
                            prob = prob*100*(1/(l_ratio**6))

                            #logger.info("fuzzy matched: {} against {} with prob {}".format(target_token,potenital_token, prob))

                            best_score = min(best_score, prob)

                        #Calculate the edit distance ratio - how many edits do we need to make as a proportion
                        #of all characters in the shortest string?

                        #If this is 0.7 or above, assume we have a

            #If we haven't found any sort of match return 1 (i.e. leave the probability unalterned)
            #logger.debug("potential token: {} returning from else {}".format(potenital_token,best_score))

            return best_score

        address.probability = reduce(lambda x,y: x*get_prob(y), list(set(address.tokens_original_order_postcode) - set(address.numbers)) + address.numbers,1.0) #Only feed tokens in once except numbers- order doesn't matter here

    def set_other_stats_on_address(self, potential_address):

        score = 1

        #Do all the numbers match?
        s1 = set(self.address_to_match.numbers)
        s2 = set(potential_address.numbers)

        if s1 != s2:
            score = score * 0.95

        #Figure out how many tokens match out of the total:
        s_atm = set(self.address_to_match.tokens_original_order_postcode)
        s_pa = set(potential_address.tokens_original_order_postcode)

        len_s_atm = len(self.address_to_match.tokens_original_order_postcode)
        len_s_pa = len(potential_address.tokens_original_order_postcode)

        matches = s_atm.intersection(s_pa)
        all_tokens = s_atm.union(s_pa)

        # Want to know how many tokens are in the potential target address which are NOT in the address to match
        # e.g. ATM is Giles' Farm, 10 Wood Lane
        # PA is caravan, field next to Giles' Farm, 10 Wood Lane

        # Essentially we want to make the score lower if there are tokens in PA which are not in ATM.  But not by much
        # Another option is the number of non matching tokens is a tie breaker.
        # For now let's make it affect the score - just not very much

        num_non_matching_tokens =  len(all_tokens) - len(matches)

        score = score -  num_non_matching_tokens*0.01


        log10 = math.log10(potential_address.probability)
        asymp_to_1 = log10*-1
        asymp_to_1 = asymp_to_1/20

        asymp_to_1 = asymp_to_1/(1+asymp_to_1)

        asymp_to_1 = (asymp_to_1**2)*3

        score = score * asymp_to_1

        potential_address.match_score = score

        score_list = [{"score": 0.9, "desc": "Very good match"},
                      {"score": 0.75, "desc": "Good match"},
                      {"score": 0.6, "desc": "Average match"},
                      {"score": 0.45, "desc": "Poor match"},
                      {"score": 0.3, "desc": "Very poor match"},
                      {"score": 0, "desc": "No match"}]

        for d in score_list:
            if potential_address.match_score >= d["score"]:
                potential_address.match_description = d["desc"]
                break

        # logger.debug(u"match stats:  score {}, desc {}".format(self.best_match.match_score, self.best_match.match_description))



    def find_match(self):
        """
        Look through the potential matches to find the one which
        is most likely to be a match 
        """

        #Basic strategy here is going to be 'probabalistic' in a loose sense
        num_addresses = len(self.potential_matches)

        for address in self.potential_matches:

            self.set_prob_on_address(address)
            self.set_other_stats_on_address(address)

        #Now just need to find the address with the highest score:
        list_of_addresses = self.potential_matches
        list_of_addresses = sorted(list_of_addresses, key=lambda x: x.match_score, reverse=True)
        self.potential_matches = list_of_addresses



        #Now we want to set statistics on the matched addresses which can only be set relative to the best match
        self.set_comparative_match_stats()

        if len(self.potential_matches)>0:
            self.best_match = self.potential_matches[0]
            try:
                logging.debug(u"\n" +  "\n".join([repr(m) for m in self.potential_matches[:5]]))
            except:
                logging.debug("log message not printed because string not ascii")



        
    #TODO Note that match stats have to be in some sense arbitrary.  Is there anything we can do to improve this?
    def set_comparative_match_stats(self):
        """
        This sets match stats which are able to be defined only comparatively (relative to the best match)
        All other match scores are determined for each potential match.
        """

        # logger.debug("adding best match scores")

        if len(self.potential_matches)> 0:

            best_score = self.potential_matches[0].match_score

            for p in self.potential_matches:
                p.relative_score = p.match_score - best_score

            if len(self.potential_matches) > 1:
                self.distinguishability = self.potential_matches[1].relative_score
            else:
                self.distinguishability = 1

