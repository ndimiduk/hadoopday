users = load '/Users/gates/tmp/amazon_hadoop_day/data/users' as (name:chararray,
        addr:chararray, city:chararray, state:chararray, zip:int);
views = load '/Users/gates/tmp/amazon_hadoop_day/data/views' as (name:chararray,
        timestamp:long, action:chararray, purchase:double);
view_with_user_info = join users by name, views by name parallel 2;
store view_with_user_info into '/Users/gates/tmp/amazon_hadoop_day/results/join';
