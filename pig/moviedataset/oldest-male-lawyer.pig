users = load 'input/users.txt' using PigStorage('|') as (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
filtered = filter users by gender == 'M' and occupation == 'lawyer';
sorted = order filtered by age desc;
limited = limit sorted 1;
userId = foreach limited generate userId;
store userId into 'oldest-male-lawyer-output';
