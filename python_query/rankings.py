from pyspark import SparkContext
import sys

'''
# spark_query.py file_path column ( < | = | > ) int
# python rankings.py hdfs://localhost:8020/tmp/benchmark/text/tiny/rankings pageRank \> 50
'''


class Rankings:
    def __init__(self, path):
        self.path = path
        sc = SparkContext()
        self.input = sc.textFile(path)
        self.ranking_table = self.input.map(lambda x: (x.split(",")[0], int(x.split(",")[1]), int(x.split(",")[2])))
        print self.ranking_table.first()

    def equal(self, column, threshold):
        result = self.ranking_table.filter(lambda x: x[column] == threshold)
        for r in result.collect():
            print r

    def less(self, column, threshold):
        result = self.ranking_table.filter(lambda x: x[column] < threshold)
        for r in result.collect():
            print r

    def greater(self, column, threshold):
        result = self.ranking_table.filter(lambda x: x[column] > threshold)
        for r in result.collect():
            print r


def main(argv):
    if len(argv) < 5:
        print argv
        print "Need five arguments"
        return

    path = argv[1]
    column = argv[2]
    comp = argv[3] 
    threshold = int(argv[4])

    if column == "pageRank":
        column_index = 1
    elif column == "avgDuration":
    	column_index = 2
    else:
    	print "Invalid volume"
    	return

    rankings = Rankings(path)
    
    if comp is "=":
        rankings.equal(column_index, threshold)
    elif comp is ">":
    	rankings.greater(column_index, threshold)
    elif comp is "<":
    	rankings.less(column_index, threshold)
    else:
        print "Invalid compare symbol"


if __name__ == "__main__":
    main(sys.argv)

