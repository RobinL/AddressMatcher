
* Could add county names to ABP addresses using the OS ceremonial/historic shapefile.  This would improve the match where the partial address includes text like 'west midlands'.  An easier but less good approach would be to remove these from the partial addresses

* Possibly punishing superfluous characters more harshly may improve results

* Sometimes we have excessive spaces or not enough newc astle or coronationstreet.  If no matches found, could experiment with removing these?

* Distinguishability could be computed vs match outside postcode rather than vs. second match

* Change the Distinguishability threshold for number of LA matches?  Currently it's hard coded...

* At the moment if a token matches twice, the score is reduced twice - this is probably wrong e.g. THE MURRAY SURGERY 50 THE MURRAY ROAD EAST KILBRIDE GLASGOW G75 0RT vs 09 TELFORD ROAD MURRAY EAST KILBRIDE
As we match tokens, pop them as we go.  So if a token matches twice, it is only scored twice if it appears twice in both addresses

* Worth removing comercial premises from the match list?  Might be worth trying - look at the false positive list

* Make one of the 'match accepted' criteria where there's a single match that a certain % of tokens match

* Make all matches try random combinations?
//


* If postcode is included, could match agaunst this

* Could makw it so that street names and locations are weighted heavier than house numbers, sometimes the alternatives have the correct street, but it matched with somewere else as it has the same house number