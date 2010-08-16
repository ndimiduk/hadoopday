users = load '/Users/gates/tmp/amazon_hadoop_day/data/users' as (name:chararray,
        addr:chararray, city:chararray, state:chararray, zip:int);
views = load '/Users/gates/tmp/amazon_hadoop_day/data/views' as (name:chararray,
        timestamp:long, action:chararray, purchase:double);
purchases = filter views by purchase is not null;
joined = join users by name, purchases by name parallel 4;
grouped = group joined by users::name parallel 2;
summed = foreach grouped generate group, SUM(joined.purchase); 
store summed into '/Users/gates/tmp/amazon_hadoop_day/results/sum';
