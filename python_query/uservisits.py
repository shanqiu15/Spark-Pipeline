from pyspark import SparkContext
import sys

'''
python uservisits.py hdfs://localhost:8020/tmp/benchmark/text/tiny/uservisits 1 9
'''

class FileHandler(object):
    def __init__(self, path):
        self.path = path
        sc = SparkContext()
        self.input = sc.textFile(path)

class UserVisits(FileHandler):
    def __init__(self, path):
        super(self.__class__, self).__init__(path)
        self.string_table = self.input.map(lambda x: x.split(","))
        self.table = self.input.map(lambda x: x.split(",")[0:3]+[float(x.split(",")[3])]+ x.split(",")[4:8] + [int (x.split(",")[8])])           
        #self.aggre_table = self.table.map(lambda x: [x[0][0:10]] + x[1:-1])

    def aggregate(self, str_start, str_end):
        self.pairs = self.table.map(lambda x: (x[0][str_start:str_end],x[3]))
        self.result = self.pairs.reduceByKey(lambda x, y: x + y)
        for r in self.result.collect():
            print r

    def stupid_show(self):
        print "the last 5 result:"
        for r in self.result.collect():
            if r[0] in [u'99.9.58.40', u'99.90.160.', u'99.92.72.8', u'99.95.1.80', u'99.96.231.']:
                print r
 
def main(argv):
    path = argv[1]
    str_start = int(argv[2]) - 1
    str_end = int(argv[3])
    user_visits = UserVisits(path)
    user_visits.aggregate(str_start, str_end)
    user_visits.stupid_show()

if __name__ == "__main__":
    main(sys.argv)

