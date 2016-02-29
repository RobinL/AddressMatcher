* Could add county names to ABP addresses using the OS ceremonial/historic shapefile.  This would improve the match where the partial address includes text like 'west midlands'
* Possibly punishing superfluous characters more harshly may improve results
* Sometimes we have excessive spaces or not enough newc astle or coronationstreet.  If no matches found, could experiment with removing these?
* Distinguishability could be computed vs match outside postcode rather than vs. second match
* At the moment if a token matches twice, the score is reduced twice - this is probably wrong e.g. THE MURRAY SURGERY 50 THE MURRAY ROAD EAST KILBRIDE GLASGOW G75 0RT vs 09 TELFORD ROAD MURRAY EAST KILBRIDE
* Worth removing comercial premises from the match list?  Might be worth trying - look at the false positive list
* Make one of the 'match accepted' criteria where there's a single match that a certain % of tokens match