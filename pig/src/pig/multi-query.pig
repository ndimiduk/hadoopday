users = load '/Users/gates/tmp/amazon_hadoop_day/data/users' as (name:chararray,
        addr:chararray, city:chararray, state:chararray, zip:int);
views = load '/Users/gates/tmp/amazon_hadoop_day/data/views' as (name:chararray,
        timestamp:long, action:chararray, purchase:double);
purchases = filter views by purchase is not null;
joined = join users by name, purchases by name parallel 4;
byzip = group joined by zip parallel 2;
zipsummed = foreach byzip generate group, SUM(joined.purchase); 
store zipsummed into '/Users/gates/tmp/amazon_hadoop_day/results/byzip';
bystate = group joined by state parallel 2;
statesummed = foreach bystate generate group, SUM(joined.purchase); 
store statesummed into '/Users/gates/tmp/amazon_hadoop_day/results/bystate';
