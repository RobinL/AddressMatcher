import pandas as pd

import sys
sys.path.append( r"C:\Users\Robin\Dropbox\Python\AddressCleaningMatching" )

from address_matcher import Matcher, Address,DataGetter_ABP,DataGetter_DPA




import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


address = Address("Bates Butchers Farm Shop, Glen Stewart Nursery, Melton Rd, Market Harborough, LE16 7TG")

data_getter_abp = DataGetter_ABP()
matcher_abp = Matcher(data_getter_abp, address)
matcher_abp.load_potential_matches()
matcher_abp.find_match()


data_getter_dpa = DataGetter_DPA()
matcher_dpa = Matcher(data_getter_dpa, address)
matcher_dpa.load_potential_matches()
matcher_dpa.find_match()
