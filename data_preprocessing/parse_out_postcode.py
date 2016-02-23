import re
import pandas as pd

def fix_postcode(address_string):

    def is_big_part(my_string):
        r1 = r"\w{3,4}"
        r2 = r".*\d.*"
        r3 = r".*[A-Z].*"
        exp_list = [r1,r2,r3]

        if all([re.match(r, my_string) for r in exp_list]):
            return True
        else:
            return False

    def is_small_part(my_string):
            if re.match("^[\dA-Z]{1,2}$", my_string):
                return True
            else:
                return False

    def is_pc_start_part(my_string):
        r = "^[A-PR-UWYZ]([0-9]{1,2}|([A-HK-Y][0-9]([0-9ABEHMNPRV-Y])?)|[0-9][A-HJKPS-UW])$"
        if re.match(r, my_string):
            return True
        else:
            return False



    def is_pc_end_part(my_string):
        r = "[0-9][ABD-HJLNP-UW-Z]{2}"
        if re.match(r, my_string):
            return True
        else:
            return False

    tokens = address_string.split(" ")

    pc_tokens = []
    while True:
        t = tokens.pop()
        if is_big_part(t) or is_small_part(t):
            pc_tokens.insert(0,t)
        else:
            tokens.append(t)
            break

        if len("".join(pc_tokens)) > 6:
            break

        if len(tokens) < 1:
            break


    # Lastly try and find valid postcode parts from the pc_tokens list
    final_pc_tokens = []
    unmatched_storage = []
    while len(pc_tokens) > 0:
        t = pc_tokens.pop()
        unmatched_storage.insert(0,t)
        whole_string = "".join(unmatched_storage)

        if is_pc_end_part(whole_string) or is_pc_start_part(whole_string):
            final_pc_tokens.insert(0,whole_string)
            unmatched_storage = []


        if len("".join(unmatched_storage))>4 or len(final_pc_tokens)>1 or len(pc_tokens) ==0:
            tokens.extend(unmatched_storage)
            break


    return " ".join(tokens+pc_tokens+final_pc_tokens)



if __name__ == "__main__":
    print fix_postcode("55 flat 2 Lon E15 22 S".upper())


