import re


def memodict(f):
    """ Memoization decorator for a function taking a single argument """
    class memodict(dict):
        def __missing__(self, key):
            ret = self[key] = f(key)
            return ret
    return memodict().__getitem__


def line_to_replace(line, address):


    address = " " + address + " "
    terms = line.upper().strip().split("|")
    correction = terms[0]
    to_correct = terms[1:]

    for t in to_correct:
        # address = re.sub(r"\b({})\b".format(t),r"{}".format(correction), address)  THIS TOOK WAY TOO LONG TO RUN
        address = address.replace(" " + t + " "," " + correction + " ")

    return address.strip()

@memodict
def get_list_of_lines(path_to_correction_data):
    with open(path_to_correction_data) as f:
        list_of_lines = f.readlines()
    return list_of_lines

def process_address_string(address, path_to_correction_data):

    list_of_lines = get_list_of_lines(path_to_correction_data)

    for l in list_of_lines:
        address = line_to_replace(l, address)

    return address

