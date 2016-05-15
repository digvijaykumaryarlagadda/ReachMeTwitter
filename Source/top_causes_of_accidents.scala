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

val mytempvar11= sqlContext.sql("SELECT 'snow', COUNT(*) AS temptable FROM tweets WHERE text like '%snow%'").collect()
val mytempvar11= sqlContext.sql("SELECT 'fog', COUNT(*) AS temptable FROM tweets WHERE text like '%fog%'").collect()
val mytempvar11= sqlContext.sql("SELECT 'rain', COUNT(*) AS temptable FROM tweets WHERE text like '%rain%'").collect()
val mytempvar11= sqlContext.sql("SELECT 'drunk', COUNT(*) AS temptable FROM tweets WHERE text like '%drunk%'").collect()
val mytempvar11= sqlContext.sql("SELECT 'mobile', COUNT(*) AS temptable FROM tweets WHERE text like '%mobile%'").collect()
val mytempvar11= sqlContext.sql("SELECT 'intoxicated', COUNT(*) AS temptable FROM tweets WHERE text like '%intoxicated%'").collect()
val mytempvar11= sqlContext.sql("SELECT 'pothole', COUNT(*) AS temptable FROM tweets WHERE text like '%pothole%'").collect()
val mytempvar11= sqlContext.sql("SELECT 'brake_failure', COUNT(*) AS temptable FROM tweets WHERE text like '%brake%'").collect()
