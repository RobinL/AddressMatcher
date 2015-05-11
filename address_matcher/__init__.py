 #!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import division

#TODO:  For simplicity can probably refactor use of pandas out of code
#to simplify the package requirements.
import pandas as pd
import functools
import re
import logging
import math
import Levenshtein

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


#TODO:  Structure into package to avoid need for this kind of stuff
import sys
sys.path.append( r"C:\Users\Robin\Dropbox\Python\AddressCleaningMatching" )


import address_functions as af
import random

#For sensitive info like db passwords.
#This also needs to be changed to relative paths within a package
import os
env_path = r"C:\Users\Robin\Dropbox\Python\AddressMatching\Address Matching\.env"
if os.path.exists(env_path):
    for line in open(env_path):
        var = line.strip().split('=')
        if len(var) == 2:
            os.environ[var[0]] = var[1]

POSTGRES_USERNAME = os.environ.get('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')


#TODO: need to control maximium size of memoize
def memoize(obj):
    cache = obj.cache = {}

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]
    return memoizer

class Address(object):

    """
    The address object holds addresses, whether they be 
    the target address (the address for which we are trying to find a match)
    or the potential matches (the list of addresses which may match the target)
    """

    def __init__(self,full_address):

        self.full_address = None
        self.full_address_no_postcode = None
        self._postcode = None
        self.business = None
        self.address_no_business = None
        self.numbers = None
        self.ordered_tokens_postcode = None
        self.ordered_tokens_no_postcode = None
        self.uprn = None
        self.probability = 1
        self.geom_wkt = None

       
        if type(full_address)== str:
            full_address = full_address.decode("utf-8")

        #get rid of any double spaces
        full_address =  re.sub("\s{2,100}", " ", full_address)
        full_address = full_address.upper()
        self.full_address = full_address
        
        pc = af.get_postcode(full_address)

        #If we assign a full address, automatically parse the postcode (note this can be overridden
        #later if we know the postcode)
        pc = af.postcode_split(pc)
        self.postcode = pc

        if self.postcode:
            self.full_address_no_postcode = full_address.replace(af.get_postcode(full_address),"")
        else:
            self.full_address_no_postcode = full_address


        #Get rid of trailing comma and space from address once postcode had been removed
        self.full_address_no_postcode =  re.sub(", $", "", self.full_address_no_postcode)
        self.numbers = af.get_numbers(self.full_address_no_postcode)

        self.set_ordered_tokens()
        

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


    def set_ordered_tokens(self):
        """
        Get the individual words which make up the address, excluding the postcode
        This will be used to run full text searches on the address.  This is mainly in
        case the postcode is wrong
        """
        tokens = af.get_tokens(self.full_address_no_postcode)
        self.ordered_tokens_no_postcode = tokens

        
        if self.postcode:
            self.ordered_tokens_postcode =  list(reversed(self.postcode.split(" "))) + tokens
        else:
            self.ordered_tokens_postcode =  tokens



    def __repr__(self):
        if self.probability:
            return u"Uprn: {0}.  Address: {1}.  Probabilty {2:.2g} ".format(self.uprn, self.full_address, self.probability)
        return self.full_address



class Matcher(object):

    """
    The matcher stores the target address and the list of potential matches
    and contains the core algorithms that perform the match and compute
    scores on the match

    Note that it doesn't deal with obtaining the list of potential matches.  
    This is delegated to a 'data_getter' class - which will be specific
    to the application.  A sample data_getter class is provided
    in DataGetter_DPA below.


    """

    def __init__(self, data_getter ,address_obj):
        """
        address_obj is the 'target address' - the one for which we want to find a match
        data_getter is a database connection to the AddressBase Premium dataset
        """

        self.data_getter = data_getter
        self.address_to_match = address_obj

        self.potential_matches = {} #This is a dict of address objects.  The keys are UPRNS
        self.best_match = Address("")
        self.second_match = Address("")
        self.distinguishability = None
        self.found = False

        self.match_score = None
        self.match_description = None

        self.fuzzy_matched_one_number = False 


    def load_potential_matches(self):
        """
        Add list of potenital matches to the matcher object.
        If only a single match is found 'short cut' the code to make it the best match
        """
        pot_matches = self.data_getter.get_potential_matches_from_tokens(self.address_to_match)
        self.potential_matches.update(pot_matches)

        #If there's a single match, assign it to the best match
        if len(self.potential_matches) ==1:  
            self.best_match = self.potential_matches.itervalues().next()
            self.set_prob(self.best_match)


    def set_prob(self,address):

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
            if potenital_token in self.address_to_match.ordered_tokens_postcode:

                return_value = main_prob

                #logger.debug("potential token: {} found {}".format(potenital_token,return_value))
                return return_value

            #if this token from one of the potetial matches is not in the target address, then maybe there's a spelling error?
            #Compare this token to each token in the target address looking for similarities

            best_score = 1

            for target_token in self.address_to_match.ordered_tokens_postcode:

                prob = self.data_getter.get_freq(target_token)

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


                    def levenshtein_ratio(str1, str2):
                        str1 = unicode(str1)
                        str2 = unicode(str2)
                        d = Levenshtein.distance(str1,str2)
                        length = max(len(str1), len(str2))
                        return 1 - (d/length)

                    #proceed to fuzzy match only if both tokens are >3 characters, otherwise best score remains 1
                    if len(target_token)> 3 and len(potenital_token)>3:
                        l_ratio = levenshtein_ratio(target_token, potenital_token)

                        #If the ratio is better than 0.7 assume it's a spelling error
                        if l_ratio>0.5:

                            prob = self.data_getter.get_freq(target_token)
                            prob = prob*100*(1/(l_ratio**6))

                            #logger.info("fuzzy matched: {} against {} with prob {}".format(target_token,potenital_token, prob))

                            best_score = min(best_score, prob)

                        #Calculate the edit distance ratio - how many edits do we need to make as a proportion
                        #of all characters in the shortest string?

                        #If this is 0.7 or above, assume we have a

            #If we haven't found any sort of match return 1 (i.e. leave the probability unalterned)
            #logger.debug("potential token: {} returning from else {}".format(potenital_token,best_score))

            return best_score

        address.probability = reduce(lambda x,y: x*get_prob(y), address.ordered_tokens_postcode,1.0)

        



    def find_match(self):
        """
        Look through the potential matches to find the one which
        is most likely to be a match 
        """

        #if one of our searches has returned a single match then short-circuit the matcher
        if len(self.potential_matches)==1:
            logger.debug(u"1st best match: {0} ".format(self.best_match))
            self.set_match_stats()
            return
        #Basic strategy here is going to be 'probabalistic' in a loose sense

        num_addresses = len(self.potential_matches.keys())


        for uprn, address in self.potential_matches.iteritems():
            self.fuzzy_matched_one_number = False
            self.set_prob(address)
            s1 = set(address.ordered_tokens_postcode)
            s2 = set(self.address_to_match.ordered_tokens_postcode)
            address.num_cannot_match = len(s1.difference(s2))
   

        #Now just need to find the address with the highest probability:
        list_of_addresses = self.potential_matches.values()
        list_of_addresses = sorted(list_of_addresses, key=lambda x: x.probability, reverse=False)
        self.potential_matches = list_of_addresses

        if len(self.potential_matches)>0:
            self.best_match =self.potential_matches[0]
        else:
            self.best_match = Address("")
        
        if len(self.potential_matches)>1:
            self.distinguishability = (self.potential_matches[1].probability/self.potential_matches[0].probability)/len(self.potential_matches)
        else:
            self.distinguishability = 1000


        self.set_match_stats() 

        logger.debug(u"1st best match: {0} is a {1} distinguishability {2:.2f}".format(self.best_match,self.match_description, self.distinguishability))
        if len(self.potential_matches)>1:
            self.second_match = self.potential_matches[1]
            logger.debug(u"2nd best match: {}".format(self.potential_matches[1]))
        else:
            self.second_match = None

        
    #TODO Note that match stats have to be in some senes arbitrary.  Is there anything we can do to improve this?
    def set_match_stats(self):
        """
        This code determines a score for the match between 0 and 1
        where 1 is excellent and 0 is very bad.
        """

        logger.debug("adding best match scores")
        if self.best_match.probability ==1:
            self.best_match.match_score=0
            self.best_match.match_description = "No match"
            logger.debug(u"match stats:  score {}, desc {}".format(self.best_match.match_score, self.best_match.match_description))
            return

        score = 1

        #Do all the numbers match?
        s1 = set(self.address_to_match.numbers)
        s2 = set(self.best_match.numbers)

        if s1 != s2:
            score = score * 0.9
        

        #What proportion of the tokens match

 
        #Figure out how many tokens match out of the total:
        s1 = set(self.address_to_match.ordered_tokens_postcode)
        s2 = set(self.best_match.ordered_tokens_postcode)

        tc1 = len(self.address_to_match.ordered_tokens_postcode)
        tc2 = len(self.best_match.ordered_tokens_postcode)

        matches = s1.intersection(s2)

        ratio =    len(matches)/min(tc1,tc2)

        if ratio == 1:
            self.best_match.match_score=1
            self.best_match.match_description = "Perfect match"


        score = score*ratio


        log10 = math.log10(self.best_match.probability)
        asymp_to_1 = log10*-1
        asymp_to_1 = asymp_to_1/20

        asymp_to_1 = asymp_to_1/(1+asymp_to_1)

        asymp_to_1 = (asymp_to_1**2)*3


        score = score * asymp_to_1

        self.best_match.match_score = score

        score_list = [{"score": 0.9, "desc": "Very good match"},
                      {"score": 0.75, "desc": "Good match"},
                      {"score": 0.6, "desc": "Average match"},
                      {"score": 0.45, "desc": "Poor match"},
                      {"score": 0.3, "desc": "Very poor match"},
                      {"score": 0, "desc": "No match"}]

        for d in score_list:
            if self.best_match.match_score >= d["score"]:
                self.best_match.match_description = d["desc"]
                break
        logger.debug(u"match stats:  score {}, desc {}".format(self.best_match.match_score, self.best_match.match_description))

        
#TODO Probably should use an abstract base class to provide the data_getter inteface,
#and all data_getters should inherit from this.

import psycopg2
from pandas.io.sql import DatabaseError
class DataGetter_ABP(object):

    """
    The Matcher class requires a DataGetter, which handles connections to the database, and retrieving records
    """

    #Probably should implement this as an abstract base class, and then write specific datagetters like this one 
    #from the ABC

    def __init__(self):


        con_string = "host='localhost' dbname='abp' user='{0}' password='{1}' options='-c statement_timeout=300'" \
                        .format(POSTGRES_USERNAME, POSTGRES_PASSWORD)
        self.con = psycopg2.connect(con_string)



        self.token_SQL = u"""
            select
                uprn,
                organisation,
                geo_single_address_label,
                ST_AsEWKT(geom) as geom_wkt,
                geom,
                postcode_locator as postcode
            from abp_useful_gb
            where to_tsvector('english',geo_single_address_label)
            @@ to_tsquery('english','{0}')
            limit {1};
            """
        


    @memoize
    def get_freq(self,term):

        term = term.lower()
        generic_SQL = """
        select * from term_frequencies where term = '{}'
        """

        SQL = generic_SQL.format(term)
        df = pd.read_sql(SQL,self.con)
        #logger.debug("hit database term freq potenital token")

        if len(df)==1:
            main_prob = df.loc[0,"freq"]
        elif len(df)==0:
            main_prob = 3.0E-7
        else:
            logger.info("returned more than one result for a single term {}".format(potenital_token.lower()))
        return main_prob


    def df_to_address_objects(self,df):

        address_list= df.to_dict(orient="records")

        return_dict = {}

        for address_dict in address_list:
            uprn = address_dict["uprn"]
            address = Address(address_dict["geo_single_address_label"])
            address.postcode = address_dict["postcode"]
            address.business = address_dict["organisation"]
            address.uprn = address_dict["uprn"]
            address.geom_wkt = address_dict["geom_wkt"]

            return_dict[uprn] =address

        return return_dict


    def get_potential_matches_from_tokens(self, address):

        """
        Does full text searches using the tokens of the address.

        Begins by searching for all the terms, then 
        deletes the most specific and searches on the remainder etc.

        Stops when it finds a result with >0 matches
        Returns nothing if all it can find is a result with > 500 matches

    
        """

        def get_potential_matches(sub_tokens):
            
            #Create a FTS SQL query using all these tokens

            search_tokens = " & ".join(sub_tokens)

            from pandas.io.sql import DatabaseError
            #logger.debug("before {}".format(search_tokens))
            SQL = self.token_SQL.format(search_tokens, limit)
            try:
                df = pd.read_sql(SQL,self.con)
            except DatabaseError as e:
                logger.debug("db error")
                df = pd.DataFrame()
            
            return df

        return_dict = {}

        logger.debug("------------")
        logger.debug(u"looking for: " + address.full_address)

        tokens = address.ordered_tokens_postcode
        limit = 75

        #If the address has three token or less, don't even try to match
        if len(tokens)<4:
            return return_dict

        #Start with full list of tokens (i.e. very specific)
        #and get more general by dropping the most specific tokens one
        #at a time until we find a match

        #Remember that the ordered tokens start with the postcode reversed.  
        #e.g. if address is 1 CHAPEL LANE, TOTTERNHOE, DUNSTABLE, LU6 2BZ
        #ordered tokens will be 2BZ LU6, 1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE etc.

        #Do a specific to general search i.e. FTS
        #2BZ LU6, 1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
        #LU6, 1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
        #1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
        #etc

        for i in range(len(tokens)):

            sub_tokens = tokens[i:]
            if len(sub_tokens)<=3:
                df= pd.DataFrame()
                break

            df = get_potential_matches(sub_tokens)

            #If there's a single match, then we've very likely found the right address.  Return just the one
            if len(df) == 1:
                return self.df_to_address_objects(df)

            if len(df)>0 and len(df)<limit:
                return_dict.update(self.df_to_address_objects(df))
                break



        #Now try going in the opposite direction - i.e. getting rid of the latter
        #parts of the address first

        #Do a specific to general search i.e. FTS
        #2BZ LU6, 1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
        #2BZ LU6, 1, CHAPEL LANE, TOTTERNHOE
        #2BZ LU6, 1, CHAPEL LANE
        #2BZ LU6, 1, CHAPEL 
        #etc
        #This ends with just a postcode search

        for i in range(1,len(tokens)):
            sub_tokens = tokens[:-i]
            if len(sub_tokens)<=2: #to make sure it ends with a postcode search
                break
            df = get_potential_matches(sub_tokens)

            #If there's a single match, then we've very likely found the right address.  Return just the one
            if len(df) == 1:
                return self.df_to_address_objects(df)

            if len(df)>0 and len(df)<limit:
                return_dict.update(self.df_to_address_objects(df))
                break



        #If we still haven't found anything make a last ditch attempt by taking random selections
        # of the tokens
        num_tokens = len(tokens)
        if len(return_dict) ==0 :  

            tried = []
            num_tokens = len(tokens)
            if num_tokens > 10:
                    take = num_tokens-5 #at least 6
            elif num_tokens > 8:
                take = num_tokens-4 #at least 5
            else:
                take = num_tokens-1


            if address.postcode:
                tokens = tokens + list(reversed(address.postcode.split(" ")))

            for i in range(400):

                #vary take number
                take2 = take - i%3
                take2 = min(num_tokens-1,take2)

                sub_tokens = random.sample(tokens, take2)
                
                if tuple(sub_tokens) in tried: 
                    continue

                df = get_potential_matches(sub_tokens)
                
                tried.append(tuple(sub_tokens))

                if len(df)>0 and len(df)<limit:
                    return_dict.update(self.df_to_address_objects(df))
                    break

        return return_dict




import psycopg2
from pandas.io.sql import DatabaseError
class DataGetter_DPA(object):

    """
    The Matcher class requires a DataGetter, which handles connections to the database, and retrieving records
    """

    #Probably should implement this as an abstract base class, and then write specific datagetters like this one 
    #from the ABC

    def __init__(self):


        con_string = "host='localhost' dbname='abp' user='{0}' password='{1}' options='-c statement_timeout=300'" \
                        .format(POSTGRES_USERNAME, POSTGRES_PASSWORD)
        self.con = psycopg2.connect(con_string)
        
        self.token_SQL = u"""
            select 
                    uprn, 
                    organisation_name, 
                    dpa_single_address_label,
                    ST_AsEWKT(geom) as geom_wkt, 
                    geom,
                    postcode as postcode
                from abp_useful_dpa 
                where to_tsvector('english',dpa_single_address_label) 
                @@ to_tsquery('english','{0}')
                limit {1};
            """
        


    @memoize
    def get_freq(self,term):

        term = term.lower()
        generic_SQL = """
        select * from term_frequencies where term = '{}'
        """

        SQL = generic_SQL.format(term)
        df = pd.read_sql(SQL,self.con)
        #logger.debug("hit database term freq potenital token")

        if len(df)==1:
            main_prob = df.loc[0,"freq"]
        elif len(df)==0:
            main_prob = 3.0E-7
        else:
            logger.info("returned more than one result for a single term {}".format(potenital_token.lower()))
        return main_prob


    def df_to_address_objects(self,df):

        address_list= df.to_dict(orient="records")

        return_dict = {}

        for address_dict in address_list:
            uprn = address_dict["uprn"]
            address = Address(address_dict["dpa_single_address_label"])
            address.postcode = address_dict["postcode"]
            address.business = address_dict["organisation_name"]
            address.uprn = address_dict["uprn"]
            address.geom_wkt = address_dict["geom_wkt"]

            return_dict[uprn] =address

        return return_dict


    def get_potential_matches_from_tokens(self, address):

        """
        Does full text searches using the tokens of the address.

        Begins by searching for all the terms, then 
        deletes the most specific and searches on the remainder etc.

        Stops when it finds a result with >0 matches
        Returns nothing if all it can find is a result with > 500 matches

    
        """

        def get_potential_matches(sub_tokens):
            
            #Create a FTS SQL query using all these tokens

            search_tokens = " & ".join(sub_tokens)
          
            #logger.debug("before {}".format(search_tokens))
            SQL = self.token_SQL.format(search_tokens, limit)
            try:
                df = pd.read_sql(SQL,self.con)
            except DatabaseError as e:
                logger.debug("db error")
                df = pd.DataFrame()

            
            return df

        return_dict = {}

        logger.debug("------------")
        logger.debug("looking for: " + address.full_address)

        tokens = address.ordered_tokens_postcode
        limit = 75

        #If the address has two token or less, don't even try to match
        if len(tokens)<3:
            return return_dict

        #Start with full list of tokens (i.e. very specific)
        #and get more general by dropping the most specific tokens one
        #at a time until we find a match

        #Remember that the ordered tokens start with the postcode reversed.  
        #e.g. if address is 1 CHAPEL LANE, TOTTERNHOE, DUNSTABLE, LU6 2BZ
        #ordered tokens will be 2BZ LU6, 1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE etc.

        #Do a specific to general search i.e. FTS
        #2BZ LU6, 1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
        #LU6, 1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
        #1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
        #etc

        for i in range(len(tokens)):

            sub_tokens = tokens[i:]
            if len(sub_tokens)<=3:
                df= pd.DataFrame()
                break

            df = get_potential_matches(sub_tokens)

            #If there's a single match, then we've very likely found the right address.  Return just the one
            if len(df) == 1:
                return self.df_to_address_objects(df)

            if len(df)>0 and len(df)<limit:
                return_dict.update(self.df_to_address_objects(df))
                break



        #Now try going in the opposite direction - i.e. getting rid of the latter
        #parts of the address first

        #Do a specific to general search i.e. FTS
        #2BZ LU6, 1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
        #2BZ LU6, 1, CHAPEL LANE, TOTTERNHOE
        #2BZ LU6, 1, CHAPEL LANE
        #2BZ LU6, 1, CHAPEL 
        #etc
        #This ends with just a postcode search

        for i in range(1,len(tokens)):
            sub_tokens = tokens[:-i]
            if len(sub_tokens)<=2: #to make sure it ends with a postcode search
                break
            df = get_potential_matches(sub_tokens)

            #If there's a single match, then we've very likely found the right address.  Return just the one
            if len(df) == 1:
                return self.df_to_address_objects(df)

            if len(df)>0 and len(df)<limit:
                return_dict.update(self.df_to_address_objects(df))
                break



        #If we still haven't found anything make a last ditch attempt by taking random selections
        # of the tokens
        num_tokens = len(tokens)

        if len(return_dict) ==0 :  

            tried = []
            num_tokens = len(tokens)
            if num_tokens > 10:
                    take = num_tokens-5 #at least 6
            elif num_tokens > 8:
                take = num_tokens-4 #at least 5
            else:
                take = num_tokens-1


            if address.postcode:
                tokens = tokens + list(reversed(address.postcode.split(" ")))

            for i in range(400):

                sub_tokens = random.sample(tokens, take)
                
                if tuple(sub_tokens) in tried: 
                    continue

                df = get_potential_matches(sub_tokens)
                
                tried.append(tuple(sub_tokens))

                if len(df)>0 and len(df)<limit:
                    return_dict.update(self.df_to_address_objects(df))
                    break

        return return_dict