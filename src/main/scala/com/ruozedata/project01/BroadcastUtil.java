package com.ruozedata.project01;

import java.util.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastUtil {
    private static String user = "root";
    private static String password = "root";
    private static String url = "jdbc:mysql://192.168.205.131:3306/onlineloganalysis";
    private static String altertable = "alertinfo_config";
    private static Date lastUpdatedAt = Calendar.getInstance().getTime();//上次time

    private static BroadcastUtil obj = new BroadcastUtil();
    private BroadcastUtil(){}
    public static BroadcastUtil getInstance() {
        return obj;
    }


    public Broadcast<List<String>> updateAndGet(SparkSession sparkSession, Broadcast<List<String>> bcAlertList){

        Date currentDate = Calendar.getInstance().getTime();  //当前time
        long diff = currentDate.getTime()-lastUpdatedAt.getTime();//time差值

        if (bcAlertList == null || diff >= 10000) { //Lets say we want to refresh every 1 min = 60000 ms
            if (bcAlertList != null)
                bcAlertList.unpersist();//删除存储

            lastUpdatedAt = new Date(System.currentTimeMillis());//再次更新上次time


            // 定义sqlcontext
            SQLContext sqlc= sparkSession.sqlContext();
//            Properties connectionProperties = new Properties();
//            connectionProperties.put("user", user);
//            connectionProperties.put("password", password);
//            Dataset<Row> alterDs = sqlc.read()
//                    .jdbc(url, altertable, connectionProperties);//读取mysql的表数据

            //根据isenable过滤高危词
            Dataset<Row> alterDs =  sqlc.read().format("jdbc")
                    .option("url", url)
                    .option("dbtable", altertable)
                    .option("user", user)
                    .option("password", password)
                    .option("driver","com.mysql.jdbc.Driver")
                    .load().filter("isenable==1").select("keywords");

            List<String> alertList= new ArrayList<String>();
            List<Row> warninfo = alterDs.collectAsList();//返回一个list对象 返回DS的所有行
            //循环add
            for(Row row_warninfo:warninfo){
                alertList.add(row_warninfo.get(0).toString());  //keywords列
            }
            //定义广播变量bcAlertList
            bcAlertList= JavaSparkContext.fromSparkContext(sparkSession.sparkContext()).broadcast(alertList);
        }

        return bcAlertList;
    }
}
