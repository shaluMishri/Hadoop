#Simple Examples of Transformation on DataFrame in Apache Spark using python.Some of the options are missing due to version
#


#from pyspark.conf import SparkConf
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

#create SparkContext and SQLContext
sc = SparkContext()
sqlContext = SQLContext(sc)

#Creating dataframe .You can use more options or create manually.
#df = sqlContext.read.json("examples/src/main/resources/people.json")

df = sqlContext.createDataFrame([{"name":"Michael"},{"name":"Andy", "age":30},{"name":"Justin", "age":19}])
>>> df2 = sqlContext.createDataFrame([{"name":"Andy"},{"name":"Pinku","height":"50"},{"name":"Michael","height":"55"}])
#agg(*exprs) : Aggregate on the entire DataFrame without groups (shorthand for df.groupBy.agg())
>>> df.agg({"age": "max"}).collect()
#+-------+----+
#|   name| age|
#+-------+----+
#|Michael|null|
#|   Andy|  30|
#| Justin|  19|
#+-------+----+


#alias(alias) : Returns a new DataFrame with an alias set.
>>> df_al1 = df.alias("df_al1")
>>> df_al1.show()
#+-------+----+
#|   name| age|
#+-------+----+
#|Michael|null|
#|   Andy|  30|
#| Justin|  19|
#+-------+----+

#coalesce(numPartitions) : Returns a new DataFrame that has exactly numPartitions partitions.
>>> df.coalesce(1).rdd.getNumPartitions()
1

#collect() : Returns all the records as a list of Row.
>>> df.collect()
[Row(name=u'Michael', age=None), Row(name=u'Andy', age=30), Row(name=u'Justin', age=19)]

#columns :Returns all column names as a list.
>>> df.columns
['name', 'age']

#count() : Returns the number of rows in this DataFrame.
>>> df.count()
3

#crossJoin(other) : Returns the cartesian product with another DataFrame.
>>> df2.select("name", "height").collect()
[Row(name=u'Andy', height=None), Row(name=u'Pinku', height=u'50'), Row(name=u'Michael', height=u'55')]
>>> df.crossJoin(df2.select("height")).select("age", "name", "height").collect()

#describe(*cols) : Computes statistics for numeric and string columns.
>>> df.describe(['age']).show()
#+-------+------------------+
#|summary|               age|
#+-------+------------------+
#|  count|                 2|
#|   mean|              24.5|
#| stddev|7.7781745930520225|
#|    min|                19|
#|    max|                30|
#+-------+------------------+

#distinct() : Returns a new DataFrame containing the distinct rows in this DataFrame.
>>> df.distinct().count()
2

#drop(*cols) : Returns a new DataFrame that drops the specified column. This is a no-op if schema doesn’t contain the given column name(s).
>>> df.drop('age').collect()
[Row(name=u'Andy'), Row(name=u'Michael')]

#dropDuplicates(subset=None) : Return a new DataFrame with duplicate rows removed, optionally only considering certain columns.
>>> df.dropDuplicates().show()

#dropna(how='any', thresh=None, subset=None) : Returns a new DataFrame omitting rows with null values. DataFrame.dropna()
>>> df.na.drop().show()

#dtypes : Returns all column names and their data types as a list.
>>> df.dtypes
[('name', 'string'), ('age', 'bigint')]

#explain(extended=False) : Prints the (logical and physical) plans to the console for debugging purpose.
>>> df.explain(True)
== Parsed Logical Plan ==
LogicalRDD [name#0,age#1L], MapPartitionsRDD[4] at applySchemaToPythonRDD at NativeMethodAccessorImpl.java:-2

== Analyzed Logical Plan ==
name: string, age: bigint
LogicalRDD [name#0,age#1L], MapPartitionsRDD[4] at applySchemaToPythonRDD at NativeMethodAccessorImpl.java:-2

== Optimized Logical Plan ==
LogicalRDD [name#0,age#1L], MapPartitionsRDD[4] at applySchemaToPythonRDD at NativeMethodAccessorImpl.java:-2

== Physical Plan ==
Scan ExistingRDD[name#0,age#1L]

#fillna(value, subset=None) : Replace null values, alias for na.fill(). DataFrame.fillna() and DataFrameNaFunctions.fill() are aliases of each other.
>>> df.na.fill(50).show()
#+-------+---+
#|   name|age|
#+-------+---+
#|Michael| 50|
#|   Andy| 30|
#| Justin| 19|
#+-------+---+

#filter(condition) : Filters rows using the given condition.
>>> df.filter("age > 10").collect()
[Row(name=u'Andy', age=30), Row(name=u'Justin', age=19)]

#first() : Returns the first row as a Row.
>>> df.first()
Row(name=u'Michael', age=None)

#groupBy(*cols) : Groups the DataFrame using the specified columns, so we can run aggregation on them. See GroupedData for all the available aggregate functions.
>>> df.groupBy().avg().collect()
[Row(avg(age)=24.5)]

#head(n=None) : Returns the first n rows.
>>> df.head()
Row(name=u'Michael', age=None)

#join(other, on=None, how=None) : Joins with another DataFrame, using the given join expression.
>>> df.join(df2, df.name == df2.name, 'outer').select(df.name, df2.height).collect()
[Row(name=u'Andy', height=None), Row(name=u'Michael', height=u'55'), Row(name=u'Justin', height=None), Row(name=None, height=u'50')]

#limit(num) : Limits the result count to the number specified.
>>> df.limit(1).collect()
[Row(name=u'Michael', age=None)]

#orderBy(*cols, **kwargs) : Returns a new DataFrame sorted by the specified column(s).
>>> df.sort(df.age.desc()).collect()
[Row(name=u'Andy', age=30), Row(name=u'Justin', age=19), Row(name=u'Michael', age=None)]

#printSchema() : Prints out the schema in the tree format.
>>> df.printSchema()
root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 
#replace(to_replace, value, subset=None) : Returns a new DataFrame replacing a value with another value. DataFrame.replace() and DataFrameNaFunctions.replace() are aliases of each other.
>>> df.na.replace(19, 20).show()
#+-------+----+
#|   name| age|
#+-------+----+
#|Michael|null|
#|   Andy|  30|
#| Justin|  20|
#+-------+----+
	
#schema : Returns the schema of this DataFrame as a pyspark.sql.types.StructType.
>>> df.schema
StructType(List(StructField(name,StringType,true),StructField(age,LongType,true)))

#select(*cols) : Projects a set of expressions and returns a new DataFrame.
>>> df.select('*').collect()
[Row(name=u'Michael', age=None), Row(name=u'Andy', age=30), Row(name=u'Justin', age=19)]

#show(n=20, truncate=True) : Prints the first n rows to the console.
>>> df2.show()
#+-------+------+
#|   name|height|
#+-------+------+
#|   Andy|  null|
#|  Pinku|    50|
#|Michael|    55|
#+-------+------+

#sort(*cols, **kwargs) : Returns a new DataFrame sorted by the specified column(s).
>>> df.sort(df.age.desc()).collect()
[Row(name=u'Andy', age=30), Row(name=u'Justin', age=19), Row(name=u'Michael', age=None)]

#sortWithinPartitions(*cols, **kwargs) : Returns a new DataFrame with each partition sorted by the specified column(s).
>>> df.sortWithinPartitions("age", ascending=False).show()
#+-------+----+
#|   name| age|
#+-------+----+
#|Michael|null|
#|   Andy|  30|
#| Justin|  19|
#+-------+----+

#subtract(other) : Return a new DataFrame containing rows in this frame but not in another frame.
>>> df.subtract(df2).show()
#+-------+----+
#|   name| age|
#+-------+----+
#|   Andy|  30|
#| Justin|  19|
#|Michael|null|
#+-------+----+

#take(num) : Returns the first num rows as a list of Row.
>>> df.take(1)
[Row(name=u'Michael', age=None)]

#toDF(*cols) : Returns a new class:DataFrame that with new specified column names
>>> df.toDF('f1', 'f2').collect()
[Row(f1=u'Michael', f2=None), Row(f1=u'Andy', f2=30), Row(f1=u'Justin', f2=19)]

#toJSON(use_unicode=True) : Converts a DataFrame into a RDD of string.
>>> df.toJSON().collect()
[u'{"name":"Michael"}', u'{"name":"Andy","age":30}', u'{"name":"Justin","age":19}']

#toLocalIterator() : Returns an iterator that contains all of the rows in this DataFrame. The iterator will consume as much memory as the largest partition in this DataFrame.

#where(condition) : where() is an alias for filter().

#withColumn(colName, col) : Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
>>> df.withColumn('age2', df.age + 20).collect()
[Row(name=u'Michael', age=None, age2=None), Row(name=u'Andy', age=30, age2=50), Row(name=u'Justin', age=19, age2=39)]
