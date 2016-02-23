def print_comparisons(df, field1= "full_address", field2="address_cleaned"):
    for r in df.iterrows():
        row = r[1]

        if row[field1] != row[field2]:
            print row[field1]
            print row[field2]
            print ""

import re
def fix_nfa(address):
    address = re.sub(r"\bN[ .]?F[ .]?A\b", "NO FIXED ABODE", address)
    return address

def add_space_to_postcode(address):
    regex = "(([A-PR-UWYZ]([0-9]{1,2}|([A-HK-Y][0-9]([0-9ABEHMNPRV-Y])?)|[0-9][A-HJKPS-UW])))([0-9][ABD-HJLNP-UW-Z]{2})"


    address = re.sub(regex, r"\2 \6", address)
    return address
