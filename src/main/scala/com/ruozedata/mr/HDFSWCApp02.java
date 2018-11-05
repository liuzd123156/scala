package com.ruozedata.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

/**
 * 使用HDFS API来完成wc统计
 *
 * 功能拆解：
 * 1）读取HDFS上的文件 ===> HDFS API
 * 2）业务逻辑处理（词频统计）：对文件中的每一行数据进行业务处理(split)  ==> Mapper
 * 3）需要将结果存起来 ==> Context
 * 4）将存起来的结果输出  ==> HDFS API
 *
 */
public class HDFSWCApp02 {

    public static void main(String[] args) throws Exception{

        Properties properties = ParamUtils.getProperties();

        // 1）读取HDFS上的文件 ===> HDFS API
        FileSystem fs = FileSystem.get(new URI(properties.get(Constants.HDFS_URI).toString()), new Configuration());
        Path path = new Path(properties.get(Constants.INPUT_PATH).toString());
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, false);

        // TODO... prepare
        RZContext context = new RZContext();

        Class<?> clazz = Class.forName(properties.get(Constants.MAPPER_CLASS).toString());

        RZMapper mapper = (RZMapper)clazz.newInstance();

        while(iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());

            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while((line = reader.readLine()) != null) {

                //  * 2）业务逻辑处理（词频统计）：对文件中的每一行数据进行业务处理(split)  ==> Mapper
                // TODO...
                // 3）需要将结果存起来 ==> Context
                mapper.map(line, context);
            }

            reader.close();
            in.close();
        }


        // 4）将存起来的结果输出  ==> HDFS API

        Path target = new Path(properties.get(Constants.OUTPUT_PATH).toString());
        FSDataOutputStream out = fs.create(new Path(target, new Path(properties.get(Constants.OUTPUT_FILE).toString())));

        Map<Object,Object> contextMap = context.getCacheMap();
        for(Map.Entry<Object,Object> entry : contextMap.entrySet()) {
            out.write((entry.getKey() + "\t" + entry.getValue() + "\n").getBytes());
        }
        out.close();
        fs.close();

        System.out.println("........哈哈..........");
    }

}
