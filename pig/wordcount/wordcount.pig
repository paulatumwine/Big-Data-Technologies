a = load 'input/InputForWC.txt' using PigStorage('\n');
b = foreach a generate REPLACE($0, '\\s+', ',');
c = foreach b generate TOKENIZE($0);
d = foreach c GENERATE flatten($0);
e = group d by $0;
f = foreach e generate group, COUNT(d);
store f into 'output';