import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print >> sys.stderr
        "Usage: PageRank.py <input-file> <output-file> <iterations>"
        sys.exit()
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def computeContribs(neighbors, rank):
        for neighbor in neighbors:
            yield(neighbor, rank/len(neighbors))

    links = spark.sparkContext.textFile(sys.argv[1]).\
        map(lambda line: line.split(',')).\
        map(lambda pages: (pages[0], pages[1])).\
        distinct().\
        groupByKey().\
        map(lambda x: (x[0], list(x[1])))
    
    ranks = links.map(lambda element: (element[0], 1.0))
    iterations = int(sys.argv[3])

    for x in range(iterations+1):
        contribs = links.join(ranks).flatMap(lambda row: computeContribs(row[1][0], row[1][1]))
        print("\n")
        print("------- Iter: " + str(x) + " --------")
        ranks = contribs.reduceByKey(lambda v1, v2: v1+v2).map(lambda x: (x[0], x[1] * 0.85 + 0.15))
        for rank in ranks.collect(): print(rank)
        print("\n")

    print("------- Final Results --------")
    for rank in ranks.collect(): print(rank)
    ranks.saveAsTextFile(sys.argv[2])
    spark.stop() 
