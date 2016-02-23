# In this script we want to apply some lookups such as ave -> avenue to the address data

# Can we build our own list of abbreviations by doing a regex on the table of tokens for - e.g. - three consonants in a row
# sometimes we see things like  DORMERHSE WARWICK OLTON BHAM WESTMIDS
# Can we somehow 'find' westmids, bham and dormerhse?

#We can get out incorrectly formatted postcodes (with too many spaces etc) by getting rid of ALL SPACES, and then regexing for the postcode
