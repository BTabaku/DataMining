import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print >> sys.stderr
        "Usage: PageRank.py <input-file> <output-file> <iterations>"
        sys.exit()
    spark = SparkContext.getOrCreate()
    spark.setLogLevel("WARN")
    stream =  StreamingContext(spark,30)

    def computeContribs(neighbors, rank):
        for neighbor in neighbors:
            yield(neighbor, rank/len(neighbors))

    links = stream.textFileStream(sys.argv[1]).\
        map(lambda line: line.split(',')).\
        map(lambda pages: (pages[0], pages[1])).\
        transform(lambda rdd: rdd.distinct()).\
        groupByKey().\
        map(lambda x: (x[0], list(x[1])))
    
    
    
    ranks = links.map(lambda element: (element[0], 1.0))
    iterations = int(sys.argv[3])
    for x in range(iterations+1):
        contribs = links.join(ranks).flatMap(lambda row: computeContribs(row[1][0], row[1][1]))
        print("\n")
        print("------- Iter: " + str(x) + " --------")
        ranks = contribs.reduceByKey(lambda v1, v2: v1+v2).map(lambda x: (x[0], x[1] * 0.85 + 0.15))
        print("\n")

    print("------- Final Results --------")
    ranks.pprint()
    
    stream.start()
    stream.awaitTermination()
