import psycopg2

from data_getters.generic_postgres import DataGetter_Postgres_Generic

from address_matcher import Address

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class DataGetter_Red_Tractor(DataGetter_Postgres_Generic):

    """
    The Matcher class requires a DataGetter, which handles connections to the database, and retrieving records
    """

    #Probably should implement this as an abstract base class, and then write specific datagetters like this one 
    #from the ABC

    def __init__(self):
      
        con_string_freq = "host='localhost' dbname='abp' user='postgres' password='fsa_password' options='-c statement_timeout=300'"
        self.freq_con = psycopg2.connect(con_string_freq)

        con_string_data = "host='localhost' dbname='matching_data' user='postgres' password='fsa_password' options='-c statement_timeout=300'"
        self.data_con = psycopg2.connect(con_string_data)

        self.token_SQL = u"""
            select 
                    id,
                    full_address
                from red_tractor
                where to_tsvector('english',full_address)
                @@ to_tsquery('english','{0}')
                limit {1};
            """

        self.freq_SQL = u"""
            select *
            from term_frequencies
            where term = '{}'
        """

        self.max_results = 75


    
    def df_to_address_objects(self,df):

        address_list= df.to_dict(orient="records")

        return_dict = {}

        for address_dict in address_list:
            id = address_dict["id"]
            address = Address(address_dict["full_address"])
            address.postcode = None
            address.business = None
            address.id = address_dict["id"]
            address.geom_wkt = None

            return_dict[id] =address

        return return_dict



