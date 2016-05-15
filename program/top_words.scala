/*
bin/spark-shell --packages com.stratio.datasource:spark-mongodb_2.10:0.11.0
*/
import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource._
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.schema._
import com.stratio.datasource.mongodb.writer._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import org.apache.spark.sql.SQLContext
import com.stratio.datasource.util.Config._
import org.apache.spark.sql.functions._
import scala.math.random
import org.apache.spark._
import org.apache.spark.SparkContext._
import sqlContext.implicits._

val builder = MongodbConfigBuilder(Map(Host -> List("MacBook-Pro-2.local:27017"), Database -> "reachme", Collection ->"tweets", SamplingRatio -> 1.0, WriteConcern -> "normal"))
val readConfig = builder.build()
val mongoRDD = sqlContext.fromMongoDB(readConfig)
mongoRDD.registerTempTable("tweets")
val mytempvar=sqlContext.sql("select text, count(*)as temptable from tweets group by text limit 1000000")
val mytempvar2=mytempvar.flatMap(line=>line.getString(0).split(" "))
val mytempvar3=mytempvar2.map(word=>(word,1)).reduceByKey((a,b)=>a+b).sortBy(-_._2).top(1000)
mytempvar3.coalesce(1,true).saveAsTextFile("topWordsOutput")


/*
 val tempvar3 = sqlContext.sql("SELECT text FROM tweets WHERE text <> '' ")
  .map(row => (row.getString(0),1)
  .reduceByKey((a, b) => a + b).sortBy(-_._2)
  
val mytempvar2 = mytempvar.select("text").flatMap(lambda row: row.text.split(" "))
val mytempvar3=mytempvar2.map(lambda w: Row(word=w, cnt=1)).toDF()
val mytempvar4=mytempvar3.groupBy("text").sum()

val mytempvar5=mytempvar.flatMap(line=>line.getString(0).split(" ")).map(word=>(word,1)).collect().take(10).foreach(println); This will get crashed as collect keep all the files in memory, but maximum allowed capacity is only 500 mb


val mytempvar5=mytempvar.flatMap(line=>line.getString(0).split(" "))
val mytempvar6=mytempvar5.map(word=>(word,1)).reduceByKey((a,b)=>a+b).sortBy(-_._2)

word => (word, 1)).reduceByKey((a, b) => a + b).sortBy(-_._2)

val mytempvar2 = mytempvar.map(word => (word, 1)).reduceByKey((a, b) => a + b).sortBy(-_._2)
val mytempvar3=mytempvar.map(word=>(word,1)).reduceByKey((a, b) => a + b).

countsRDD = (mytempvar2
             .flatMap(lambda tweet: [hashtag['text'].lower() for hashtag in tweet['entities']['hashtags']])
             .map(lambda tag: (tag, 1))
             .reduceByKey(lambda a, b: a + b)
            )

*/