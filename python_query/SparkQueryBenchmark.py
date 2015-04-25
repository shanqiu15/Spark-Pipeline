from pyspark import SparkContext
import sys
import time
import datetime

'''
# spark_query.py file_path column ( < | = | > ) int
# python SparkQueryBenchmark.py amplab/text/tiny/rankings 50 amplab/text/tiny/uservisits 1 9 1980-01-01 1980-04-01

'''


class RankingTable:
    def __init__(self, ranking_table):
        self.ranking_table = ranking_table

    def equal(self, column, threshold):
        result = self.ranking_table.filter(lambda x: int(x[column]) == threshold)
        return result
        
    def less(self, column, threshold):
        result = self.ranking_table.filter(lambda x: int(x[column]) < threshold)
        return result

    def greater(self, column, threshold):
        result = self.ranking_table.filter(lambda x: int(x[column]) > threshold)
        return result

def scan_query(ranking_table, column=1, threshold=50):
    rankings = RankingTable(ranking_table)
    filter_result = rankings.greater(column, threshold)
    result=filter_result.map(lambda x:(x[0],x[1]))
    result.sortByKey()
    for r in result.collect():
        print r

def aggregate_query(uservisit_tables, str_start, str_end):
    uservisit_pairs = uservisit_tables.map(lambda x: (x[0][str_start:str_end],float(x[3])))
    #for r in uservisit_pairs.collect():
    #    print r
    result = uservisit_pairs.reduceByKey(lambda x, y: x + y)
    result.sortByKey()
    for r in result.collect():
        print r

def time_filter(uservisit_table,first_day, last_day):
    fmt = '%Y-%m-%d'
    start_date = datetime.datetime.strptime(first_day, fmt)
    end_date = datetime.datetime.strptime(last_day, fmt)
    return uservisit_table.filter(lambda x: start_date <= datetime.datetime.strptime(x[2], fmt) <= end_date)

if __name__ == "__main__":

    sc = SparkContext()
    #Prepare for the ranking table
    ranking_input = sc.textFile(sys.argv[1])
    ranking_table = ranking_input.map(lambda x: x.split(","))
    start_time = time.time()
    scan_query(ranking_table,1, int(sys.argv[2]))
    end_time = time.time()
    scan_execution_time = end_time - start_time

    str_start = int(sys.argv[4]) - 1
    str_end = int(sys.argv[5])
    uservisit_input = sc.textFile(sys.argv[3])
    uservisit_table = uservisit_input.map(lambda x: x.split(","))
    start_time = time.time()
    uservisit_pairs = aggregate_query(uservisit_table, str_start, str_end)
    end_time = time.time()
    aggregate_execution_time = end_time - start_time

    first_day = sys.argv[6]
    last_day = sys.argv[7]
    start_time = time.time()
    valid_date = time_filter(uservisit_table,first_day, last_day)
    ranking_pairs = ranking_table.map(lambda x: (x[0], x))
    uservisit_pairs = valid_date.map(lambda x: (x[1], x))
    join_result = uservisit_pairs.join(ranking_pairs)
    sourceIP_as_key = join_result.map(lambda x:(x[1][0][0], [int(x[1][1][1]), float(x[1][0][3])]))
    group_by_sourceIP = sourceIP_as_key.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (add_each_element(x[0], y[0]), x[1] + y[1]))
    # #group_result = (totalRevenue, (avgPageRank, sourceIP))
    group_result = group_by_sourceIP.map(lambda x: (x[1][0][1], (float(x[1][0][0])/x[1][1], x[0])))
    result =  group_result.sortByKey(False) #sort by descending order
    print result.take(1)
    end_time = time.time()
    join_execution_time = end_time - start_time
    print "The execution time of the scan query is ", scan_execution_time, "s"
    print "The execution time of the aggregate query is ", aggregate_execution_time, "s"
    print "The execution time of the join query is ", join_execution_time, "s"