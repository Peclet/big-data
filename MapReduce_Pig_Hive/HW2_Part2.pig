p2 = Load 'p2.txt' USING PigStorage('\t') AS (ngram:chararray, year:int, occurrences:double, books:double);
P2_Grouped= GROUP p2 by ngram;
P2_Averaged= FOREACH P2_Grouped{ sum_occur = SUM(p2.occurrences); sum_books = SUM(p2.books); division = (double)(sum_occur/sum_books); GENERATE group, division as sum_occurbooks;}
P2_Rounded = FOREACH P2_Averaged{ Rounded = (double)ROUND(sum_occurbooks*1000.0)/1000.0; GENERATE group, Rounded as Final;}
Order_Alphabetically = ORDER P2_Rounded BY group DESC;
Order_Maximum = ORDER Order_Alphabetically BY Final DESC;
P2_Limit = LIMIT Order_Maximum 15;
STORE P2_Limit INTO 'HW2/HW2_Q2_2.txt' using PigStorage('\t');

~                                                                               
~                                                                               
~                                                                               
~                                                                               
~                                                                               
~                                                                               
~                                                                               
~                                                                               
~                                                                               
~                                                                               
-- INSERT --                                                  9,1           All
