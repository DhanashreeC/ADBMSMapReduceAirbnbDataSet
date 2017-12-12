query_data = LOAD '/home/dhanashree/info7275/input/listings_analysis.csv' USING PigStorage(',');
filtered_data = FILTER query_data BY ($4 IS NOT NULL);
grouped_data = GROUP filtered_data BY $4;
summed = FOREACH grouped_data GENERATE group, COUNT(filtered_data) AS views;
sorted = ORDER summed BY views desc;
STORE sorted INTO '/home/dhanashree/info7275/output';
