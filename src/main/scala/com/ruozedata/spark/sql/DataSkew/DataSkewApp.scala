package com.ruozedata.spark.sql.DataSkew

import org.apache.spark.sql.SparkSession

import scala.util.Random

object DataSkewApp {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder()
                .master("local[2]")
                .enableHiveSupport()
                .appName("DataSkewApp")
                .getOrCreate()

        //加载产品数据
        val products = spark.read.format("jdbc").options(Map("url" -> "jdbc:mysql://192.168.205.131:3306",
            "user" -> "root", "password" -> "root",
            "driver" -> "com.mysql.jdbc.Driver",
            "dbtable" -> "business.product_info")).load()
        /**
          * +----------+------------+--------------------+
          * |product_id|product_name|         extend_info|
          * +----------+------------+--------------------+
          * |         1|    product1|{"product_status":1}|
          * |         2|    product2|{"product_status":1}|
          * |         3|    product3|{"product_status":1}|
          * |         4|    product4|{"product_status":1}|
          * |         5|    product5|{"product_status":1}|
          * |         6|    product6|{"product_status":1}|
          * |         7|    product7|{"product_status":1}|
          * |         8|    product8|{"product_status":1}|
          * |         9|    product9|{"product_status":0}|
          * |        10|   product10|{"product_status":1}|
          * |        11|   product11|{"product_status":0}|
          * |        12|   product12|{"product_status":0}|
          * |        13|   product13|{"product_status":0}|
          * |        14|   product14|{"product_status":0}|
          * |        15|   product15|{"product_status":1}|
          * |        16|   product16|{"product_status":0}|
          * |        17|   product17|{"product_status":1}|
          * |        18|   product18|{"product_status":0}|
          * |        19|   product19|{"product_status":1}|
          * |        20|   product20|{"product_status":1}|
          * +----------+------------+--------------------+
          */

        //加载城市数据
        val citys = spark.read.format("jdbc").options(Map("url" -> "jdbc:mysql://192.168.205.131:3306",
            "user" -> "root", "password" -> "root",
            "driver" -> "com.mysql.jdbc.Driver",
            "dbtable" -> "business.city_info")).load()
        /**
          * +-------+---------+----+
          * |city_id|city_name|area|
          * +-------+---------+----+
          * |      1|  BEIJING|  NC|
          * |      2| SHANGHAI|  EC|
          * |      3|  NANJING|  EC|
          * |      4|GUANGZHOU|  SC|
          * |      5|    SANYA|  SC|
          * |      6|    WUHAN|  CC|
          * |      7| CHANGSHA|  CC|
          * |      8|     XIAN|  NW|
          * |      9|  CHENGDU|  SW|
          * |     10|  HAERBIN|  NE|
          * +-------+---------+----+
          */

        //加载用户点击数据
        val users = spark.sql("select * from test.user_click")

        /**
          * +-------+--------------------+--------------------+-------+----------+----------+
          * |user_id|          session_id|         action_time|city_id|product_id|create_day|
          * +-------+--------------------+--------------------+-------+----------+----------+
          * |     95|2bf501a7637549c8...|2016-05-05 21:01:56|      1|        72|2016-05-05|
          * |     95|2bf501a7637549c8...|2016-05-05 21:52:26|      1|        68|2016-05-05|
          * |     95|2bf501a7637549c8...|2016-05-05 21:17:03|      1|        40|2016-05-05|
          * |     95|2bf501a7637549c8...|2016-05-05 21:32:07|      1|        21|2016-05-05|
          * |     95|2bf501a7637549c8...|2016-05-05 21:26:06|      1|        63|2016-05-05|
          * |     95|2bf501a7637549c8...|2016-05-05 21:03:11|      1|        60|2016-05-05|
          * |     95|2bf501a7637549c8...|2016-05-05 21:43:43|      1|        30|2016-05-05|
          * |     95|2bf501a7637549c8...|2016-05-05 21:09:58|      1|        96|2016-05-05|
          * |     95|2bf501a7637549c8...|2016-05-05 21:18:45|      1|        71|2016-05-05|
          * |     95|2bf501a7637549c8...|2016-05-05 21:42:39|      1|         8|2016-05-05|
          * +-------+--------------------+--------------------+-------+----------+----------+
          */
        //强制拆分数据份数,从0开始,所以加一
        val randomScope = 9
        val result = users.join(citys, "city_id").join(products, "product_id").select("area", "product_name")
        spark.udf.register("addRandomStr", (x: String) => {
            getRandomInt(randomScope) + "-" + x
        })
        spark.udf.register("removeRandomStr", (x: String) => {
            x.split("-")(1)
        })
        result.createOrReplaceTempView("result")
        //注意添加前綴的對象，如果只是少数key偏移，可以把少数key通过where条件单独查询处理，然后结果union
        spark.sql("select area,addRandomStr(product_name) as product_name,count(1) click_num " +
                "from result group by area,product_name")
                .createOrReplaceTempView("result2")
        spark.sql("select * from (select area,product_name,click_num,row_number() over(partition by area order by click_num desc) rank" +
                " from (select area,removeRandomStr(product_name) as product_name,sum(click_num) as click_num " +
                "from result2 group by area,product_name)) where rank<4").show()

        /**
          * +----+------------+---------+----+
          * |area|product_name|click_num|rank|
          * +----+------------+---------+----+
          * |  SC|   product38|       35|   1|
          * |  SC|   product33|       34|   2|
          * |  SC|   product98|       34|   3|
          * |  NW|   product67|       20|   1|
          * |  NW|   product56|       20|   2|
          * |  NW|   product48|       19|   3|
          * |  SW|   product16|       20|   1|
          * |  SW|   product95|       19|   2|
          * |  SW|   product60|       19|   3|
          * |  NC|    product9|       16|   1|
          * |  NC|   product40|       16|   2|
          * |  NC|   product94|       13|   3|
          * |  EC|    product4|       40|   1|
          * |  EC|   product96|       32|   2|
          * |  EC|   product99|       31|   3|
          * |  CC|    product7|       39|   1|
          * |  CC|   product26|       39|   2|
          * |  CC|   product70|       38|   3|
          * +----+------------+---------+----+
          */
    }

    def getRandomInt(n: Int): Int = {
        Random.nextInt(n)
    }
}
