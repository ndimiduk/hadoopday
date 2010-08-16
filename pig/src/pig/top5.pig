users = load '/Users/gates/tmp/amazon_hadoop_day/data/users' as (name:chararray,
        addr:chararray, city:chararray, state:chararray, zip:int);
views = load '/Users/gates/tmp/amazon_hadoop_day/data/views' as (name:chararray,
        timestamp:long, action:chararray, purchase:double);
pusers = foreach users generate name;
pviews = foreach views generate name, purchase;
purchases = filter pviews by purchase is not null;
joined = join pusers by name, purchases by name parallel 4;
grouped = group joined by pusers::name parallel 2;
topk = foreach grouped {
       sorted = order joined by purchase desc;
       lim = limit sorted 5;
       generate flatten(lim) as top5;
}
final = foreach topk generate $0, $2;
store final into '/Users/gates/tmp/amazon_hadoop_day/results/top5';

