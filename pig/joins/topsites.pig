users = load 'input/users.csv' using PigStorage(',') as (name:chararray,age:int);
filteredUsers = filter users by age >= 18 and age <= 25;

pages = load 'input/pages.csv' using PigStorage(',') as (name:chararray,site:chararray);

joined = join filteredUsers by name, pages by name;

grouped = group joined by site;
summed = foreach grouped generate group, COUNT(joined);
sorted = ORDER summed BY $1 desc;
top5 = LIMIT sorted 5;

store top5 into 'output';