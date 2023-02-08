# Interview

# Approach 1
Approach 1 first process each record, accumulate the daily results, and at lalst get the maxiumum value of each month.

This approach is easier to follow and read through, however it needs to save and loop through multiple lists.

# Approach 2
Approach 2 calculate the maximum while processing each record, i.e. accumulate the exposure of each day and keep updating the monthly maximum exposure. 

Although with this approach we only need to loop once, the downside is that we need to maintain the state so it might be error prone.

# Results
Results of the two approaches are slightly different, this was done on purpose as I wasn't entirey sure if we would like to include the date when no events take place.

E.g. there is no events on 1st July, the exposure of 1st July is higher than 3rd July,
the first approach took the exposure of 1st July where the second approach ignored 1st July
as no events took place (resulted in several days in a row share the same exposure).

Approach 1
January: 3045192.0
February: 3807090.6999999997
March: 4376406.66
April: 4198108.789999999
May: 1011360.4199999998
June: 180953.74999999983
July: -18513.52000000018
August: -41007.70000000018
September: -41007.70000000018
October: -41007.70000000018
November: -41007.70000000018
December: -41007.70000000018

Approach 2
January: 3045192.0
February: 3807090.6999999993
March: 4376406.659999998
April: 4198108.789999999
May: 1011360.4200000006
June: 180953.7500000007
July: -24927.339999999305
August: 0.0
September: 0.0
October: 0.0
November: 0.0
December: 0.0
