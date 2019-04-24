//
// CS 245 Assignment 2, Part I starter code
//
// 1. Loads Cities and Countries as dataframes and creates views
//    so that we can issue SQL queries on them.
// 2. Runs 2 example queries, shows their results, and explains
//    their query plans.
//

// note that we use the default `spark` session provided by spark-shell
val cities = (spark.read
        .format("csv")
        .option("header", "true") // first line in file has headers
        .load("./Cities.csv"));
cities.createOrReplaceTempView("Cities")

val countries = (spark.read
        .format("csv")
        .option("header", "true")
        .load("./Countries.csv"));
countries.createOrReplaceTempView("Countries")

// look at the schemas for Cities and Countries
cities.printSchema()
countries.printSchema()

// Example 1
var df = spark.sql("SELECT city FROM Cities")
df.show()  // display the results of the SQL query
df.explain(true)  // explain the query plan in detail:
                  // parsed, analyzed, optimized, and physical plans

// Example 2
df = spark.sql("""
    SELECT *
    FROM Cities
    WHERE temp < 5 OR true
""")
df.show()
df.explain(true)
