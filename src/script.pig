users = LOAD '/src/data.csv' USING PigStorage(',') 
    AS (id:int, name:chararray, age:int);
DUMP users;