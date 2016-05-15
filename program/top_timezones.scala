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
val mytempvar= sqlContext.sql("SELECT user.time_zone FROM tweets")
/*
val mytempvar= sqlContext.sql("SELECT user.time_zone, COUNT(*) AS temptable FROM tweets GROUP BY user.time_zone ORDER BY mytemptable DESC LIMIT 10").collect().foreach(println)
*/
val mytempvar2 = mytempvar.map(t => (t(0),1)).reduceByKey(_+_).sortBy(-_._2)
mytempvar2.coalesce(1,true).saveAsTextFile("topTimezonesOutput")