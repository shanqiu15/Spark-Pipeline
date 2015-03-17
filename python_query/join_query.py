from pyspark import SparkContext
import sys
import datetime

'''
python join_query.py hdfs://localhost:8020/tmp/benchmark/text/tiny/rankings hdfs://localhost:8020/tmp/benchmark/text/tiny/uservisits 1980-01-01 1980-04-01
'''

class Tables():
    def __init__(self, r_path, u_path, first_day, last_day):
        sc = SparkContext()
        self.ranking = sc.textFile(r_path)
        self.uservisit = sc.textFile(u_path)
        self.ranking_table = self.ranking.map(lambda x: x.split(","))
        self.uservisit_table = self.uservisit.map(lambda x: x.split(","))
        self.valid_date = self.time_filter(first_day, last_day)

    def join_table(self, index_1 = 0, index_2 = 1):
        self.ranking_pairs = self.ranking_table.map(lambda x: (x[index_1], x))
        self.uservisit_pairs = self.valid_date.map(lambda x: (x[index_2], x))
   
        #join_result data structure
        #(key = destURL, value = [uservisit, ranking])
        return self.uservisit_pairs.join(self.ranking_pairs)

    def time_filter(self, first_day, last_day):
        fmt = '%Y-%m-%d'
        start_date = datetime.datetime.strptime(first_day, fmt)
        end_date = datetime.datetime.strptime(last_day, fmt)
        return self.uservisit_table.filter(lambda x: start_date <= datetime.datetime.strptime(x[2], fmt) <= end_date)

def add_each_element(x, y):
    return [x[i] + y[i] for i in xrange(len(x))]

def join_query(argv):
    r_path = argv[1]
    u_path = argv[2]
    first_day = argv[3]
    last_day = argv[4]
    tables = Tables(r_path, u_path, first_day, last_day)
    join_result = tables.join_table()
    
    #x[1][0][0]: source ip, x[1][1][1]: pageRank, x[1][0][3]: adRevenue
    #(sourceIP, [pageRank, adRevenue]) [(u'76.185.254.234', [25, 0.77573060000000005])...]
    sourceIP_as_key = join_result.map(lambda x:(x[1][0][0], [int(x[1][1][1]), float(x[1][0][3])]))
    group_by_sourceIP = sourceIP_as_key.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (add_each_element(x[0], y[0]), x[1] + y[1]))
    #group_result = (totalRevenue, (avgPageRank, sourceIP))
    group_result = group_by_sourceIP.map(lambda x: (x[1][0][1], (float(x[1][0][0])/x[1][1], x[0])))
    return  group_result.sortByKey(False) #sort by descending order

if __name__ == "__main__":
   join_result = join_query(sys.argv)
   result = join_result.take(1)
   print str(result[0][1][1]) + ", " + str(result[0][0]) + ", " + str(result[0][1][0])
