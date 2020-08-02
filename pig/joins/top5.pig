users = LOAD 'users.csv' AS (name:chararray,age:int);
filteredUsers = FILTER users BY age >= 18 AND age <= 25;
pages = LOAD 'pages.csv' AS (name:chararray,site:chararray);
joined = JOIN filteredUsers BY name, pages BY name;
grouped = GROUP joined BY site;
summed = FOREACH grouped GENERATE site, COUNT(sites);
sorted = ORDER summed BY $1;
limit = LIMIT sorted 5;
DUMP limit;