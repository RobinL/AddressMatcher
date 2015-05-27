__author__ = 'RLinacre'


from address_matcher import Matcher, Address
from data_getters.abp2 import DataGetter_ABP2

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

data_getter_abp2 = DataGetter_ABP2()

address = Address("Food Standards Agency Aviation House, 125 Kingsway, London WC2B 6NH")

matcher_abp = Matcher(data_getter_abp2,address)
matcher_abp.load_potential_matches()
matcher_abp.find_match()
