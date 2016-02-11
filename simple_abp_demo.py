__author__ = 'RLinacre'

from address_matcher import Matcher, Address
from data_getters.abp import DataGetter_ABP

import logging

logging.root.setLevel("DEBUG")

data_getter_abp = DataGetter_ABP()

address = Address("Ministry of Justice France")

matcher_abp = Matcher(data_getter_abp,address)
matcher_abp.load_potential_matches()
matcher_abp.find_match()
matcher_abp.best_match