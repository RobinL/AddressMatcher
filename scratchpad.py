from data_preprocessing.use_dicts_to_normalise_address import process_address_string
path = "/Users/robinlinacre/Documents/python_projects/partial_address_matcher/data_preprocessing/data/custom_data.txt"

print process_address_string("154 BELFIELD RD ACCRINGTTON".upper(), path)
print process_address_string("154 BELFIELD RD ACCRINGTTON".upper(), path)