register /Users/gates/tmp/amazon_hadoop_day/src/SessionAnalysis.jar;

views = load '/Users/gates/tmp/amazon_hadoop_day/data/views' as (name:chararray,
        timestamp:long, action:chararray, purchase:double);
grouped = group views by name parallel 4;
sessions = foreach grouped {
       sorted = order views by timestamp;
       generate group, SessionAnalysis(sorted.action, sorted.timestamp) as steps;
}
describe sessions;
