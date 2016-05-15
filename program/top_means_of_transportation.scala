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

val mytempvar1= sqlContext.sql("SELECT 'car', COUNT(*) AS temptable FROM tweets WHERE text like '%car%'").collect()
val mytempvar2= sqlContext.sql("SELECT 'bike', COUNT(*) AS temptable FROM tweets WHERE text like '%bike%'").collect()
val mytempvar3= sqlContext.sql("SELECT 'truck', COUNT(*) AS temptable FROM tweets WHERE text like '%truck%'").collect()
val mytempvar4= sqlContext.sql("SELECT 'train', COUNT(*) AS temptable FROM tweets WHERE text like '%train%'").collect()
val mytempvar5= sqlContext.sql("SELECT 'bus', COUNT(*) AS temptable FROM tweets WHERE text like '%bus%'").collect()
val mytempvar6= sqlContext.sql("SELECT 'ferry', COUNT(*) AS temptable FROM tweets WHERE text like '%ferry%'").collect()
val mytempvar7= sqlContext.sql("SELECT 'helicopter', COUNT(*) AS temptable FROM tweets WHERE text like '%helicopter%'").collect()
val mytempvar8= sqlContext.sql("SELECT 'ship', COUNT(*) AS temptable FROM tweets WHERE text like '%ship%'").collect()
val mytempvar9= sqlContext.sql("SELECT 'yacht', COUNT(*) AS temptable FROM tweets WHERE text like '%yacht%'").collect()
val mytempvar10= sqlContext.sql("SELECT 'automobile', COUNT(*) AS temptable FROM tweets WHERE text like '%automobile%'").collect()
val mytempvar11= sqlContext.sql("SELECT 'plane', COUNT(*) AS temptable FROM tweets WHERE text like '%plane%'").collect()
val mytempvar12= sqlContext.sql("SELECT 'pedestrian', COUNT(*) AS temptable FROM tweets WHERE text like '%pedestrian%'").collect()

/*
val mytempvara=mytempvar1.map(l=>(l(0),l(1)))
val mytempvarb=mytempvar2.map(l=>(l(0),l(1)))
val mytempvarc=mytempvar3.map(l=>(l(0),l(1)))
val mytempvard=mytempvar4.map(l=>(l(0),l(1)))
val mytempvare=mytempvar5.map(l=>(l(0),l(1)))
val mytempvarf=mytempvar6.map(l=>(l(0),l(1)))
val mytempvarg=mytempvar7.map(l=>(l(0),l(1)))
val mytempvarh=mytempvar8.map(l=>(l(0),l(1)))
val mytempvari=mytempvar9.map(l=>(l(0),l(1)))
val mytempvarj=mytempvar10.map(l=>(l(0),l(1)))
val mytempvark=mytempvar11.map(l=>(l(0),l(1)))
val mytempvarl=mytempvar12.map(l=>(l(0),l(1)))

mytempvara.saveAsTextFile("topTransportationOutput1")
mytempvarb.saveAsTextFile("topTransportationOutput2")
mytempvarc.saveAsTextFile("topTransportationOutput3")
mytempvard.saveAsTextFile("topTransportationOutput4")
mytempvare.saveAsTextFile("topTransportationOutput5")
mytempvarf.saveAsTextFile("topTransportationOutput6")
mytempvarg.saveAsTextFile("topTransportationOutput7")
mytempvarh.saveAsTextFile("topTransportationOutput8")
mytempvari.saveAsTextFile("topTransportationOutput9")
mytempvarj.saveAsTextFile("topTransportationOutput10")
mytempvark.saveAsTextFile("topTransportationOutput11")
mytempvarl.saveAsTextFile("topTransportationOutput12")


val options = Map("host" -> "MacBook-Pro-2.local:27017", "database" -> "reachme", "collection" -> "retweetedTweetsOutput")
mytempvar.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(options).save()
"SELECT text, COUNT(*) AS temptable FROM tweets GROUP BY text ORDER BY temptable DESC LIMIT 10"

val mytempvar= sqlContext.sql("SELECT first_value(text), COUNT(*) AS temptable FROM tweets WHERE text like '%car%'").collect()



val mytempvar2 = mytempvar.filter(col("text").like("%car%"))
val mytempvar3 = mytempvar2.map(t => (t(0),1)).reduceByKey(_+_).sortBy(-_._2)

*/



