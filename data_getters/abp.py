import psycopg2

from data_getters.generic_postgres import DataGetter_Postgres_Generic

from address_matcher import Address
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class DataGetter_ABP(DataGetter_Postgres_Generic):

    """
    The Matcher class requires a DataGetter, which handles connections to the database, and retrieving records
    """


    def __init__(self):
      
        con_string_freq = "host='localhost' dbname='abp' user='postgres' password='' options='-c statement_timeout=1000'"
        self.freq_con = psycopg2.connect(con_string_freq)

        con_string_data = "host='localhost' dbname='abp' user='postgres' password='' options='-c statement_timeout=1000'"
        self.data_con = psycopg2.connect(con_string_data)

        self.token_SQL = u"""
            select 
                    uprn,
                    full_address, 
                    ST_X (ST_Transform (geom, 4326)) as lng,
                    ST_Y (ST_Transform (geom, 4326)) as lat,
                    ST_AsEWKT(geom) as geom_wkt,
                    classification_code
                from all_addresses
                where 
                to_tsvector('english',full_address)
                @@ to_tsquery('english','{0}')
                limit {1};
            """

        self.freq_SQL = u"""
                    select *
                    from term_frequencies
                    where term = '{}'
                """

        self.max_results = 101


    
    def df_to_address_objects(self,df):

        address_list= df.to_dict(orient="records")
        return_list = []

        for address_dict in address_list:
            id = address_dict["uprn"]
            address = Address(address_dict["full_address"])
            address.postcode = None
            address.business = None
            address.id = address_dict["uprn"]
            address.lat = address_dict["lat"]
            address.lng = address_dict["lng"]
            address.geom_wkt = address_dict["geom_wkt"]
            address.classification_code = address_dict["classification_code"]

            return_list.append(address)

        return return_list



