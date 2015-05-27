
from address_matcher.data_getter_abc import DataGetterABC
from address_matcher import Address

import logging
import pandas as pd
import functools
import random


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

#For sensitive info like db passwords.
#This also needs to be changed to relative paths within a package
import os
env_path = r".env"
if os.path.exists(env_path):
    for line in open(env_path):
        var = line.strip().split('=')
        if len(var) == 2:
            os.environ[var[0]] = var[1]

POSTGRES_USERNAME = os.environ.get('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

import psycopg2
from pandas.io.sql import DatabaseError
class DataGetter_ABP(DataGetterABC):

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
        try:
            df = pd.read_sql(SQL,self.con)
        except:
            df = pd.DataFrame()
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


    def get_potential_matches_from_address(self, address):

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
class DataGetter_DPA(DataGetterABC):

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
        try:
            df = pd.read_sql(SQL,self.con)
        except:
            df = pd.DataFrame()
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


    def get_potential_matches_from_address(self, address):

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