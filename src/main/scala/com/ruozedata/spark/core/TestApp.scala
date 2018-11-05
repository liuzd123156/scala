package com.ruozedata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object TestApp {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("LogApp").setMaster("local[2]")
        val sc = new SparkContext(sparkConf)
        val data = sc.parallelize(1 to 100000,1)
        val time1 = System.currentTimeMillis()
        data.map(_*2).foreach(println)
        val time2 = System.currentTimeMillis()
        data.mapPartitions(x=>{
            val list = ListBuffer[Int]()
            while(x.hasNext){
                list += x.next()*2
            }
            list.iterator
        }).foreach(println)
        val time3 = System.currentTimeMillis()
        println((time2-time1)+"   "+(time3-time2))

        /**
          * 需求：公共资源交易控标倾向计算，包括投标数据、中标数据
          * 样例数据：标段代码   中标单位代码
          *          标段代码   投标单位代码
          */
        val tb =  sc.textFile("hdfs://hadoop01:9000/user/hadoop/EMP_COLUMN_QUERY/*")
        val tb2 = tb.distinct().filter(_.length>10)//去除重复记录和初步筛选异常记录
        val tb3 = tb2.map(x=>{val s = x.split("\t")
            var s0,s1 = ""
            try{s0=s(0)//try去除异常数据影响
            s1=s(1)
            }
            catch{
                case e:Exception => e
            }
            (s0,s1)
        })
        val tb4 = tb3.groupByKey()//根据标段分组，投了同一个标的公司分成一组


        val zb = sc.textFile("hdfs://hadoop01:9000/user/hadoop/zhongbiaojieguo/*")
        val zb2 = zb.distinct().filter(_.length>10)
        val zb3 = zb2.map(x=>{val s = x.split("\t")
            var s0,s1 = ""
            try{s0=s(0)
            s1=s(1)
            }
            catch{
                case e:Exception => e
            }
            (s0,s1)
        })

        val result = zb3.join(tb4).flatMap(x=>{//中标和投标关联，然后把中标公司和投标公司列表关键建立
            val z = x._2._1
            val array = new ArrayBuffer[(String,String)]()
            for (y <- x._2._2.iterator){
                if(z.length>5&&y.length>5&&z!=y){
                    array.append((z,y))
                }
            }
            array
        }).mapPartitions(x=>{//明确投标公司和中标公司关系
            val array2 = new ArrayBuffer[((String,String),Int)]()
            while(x.hasNext){
                array2.append((x.next(),1))
            }
            array2.iterator
        }).reduceByKey(_+_,1).sortBy(_._2,false)//中标和投标公司关系深度排序
        result.take(10).foreach(println)
        result.filter(_._2.toInt>1)
        sc.stop()
    }
}
