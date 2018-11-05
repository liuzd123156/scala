package com.ruozedata.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

/**
 * 使用HDFS API来完成wc统计
 * <p>
 * 功能拆解：
 * 1）读取HDFS上的文件 ===> HDFS API
 * 2）业务逻辑处理（词频统计）：对文件中的每一行数据进行业务处理(split)  ==> Mapper
 * 3）需要将结果存起来 ==> Context
 * 4）将存起来的结果输出  ==> HDFS API
 */
public class HDFSWCApp01 {

    public static void main(String[] args) throws Exception {
        // 1）读取HDFS上的文件 ===> HDFS API
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.199.151:8020"), new Configuration());
        Path path = new Path("/wc/input/");
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, false);

        // TODO... prepare
        RZContext context = new RZContext();
        RZMapper mapper = new CaseIngoreWordCountMapper();

        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());

            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = reader.readLine()) != null) {

                // 2）业务逻辑处理（词频统计）：对文件中的每一行数据进行业务处理(split)  ==> Mapper
                // TODO...
                // 3）需要将结果存起来 ==> Context
                mapper.map(line, context);
            }

            reader.close();
            in.close();
        }


        // 4）将存起来的结果输出  ==> HDFS API

        Path target = new Path("/wc/out/");
        FSDataOutputStream out = fs.create(new Path(target, new Path("wc.out")));

        Map<Object, Object> contextMap = context.getCacheMap();
        for (Map.Entry<Object, Object> entry : contextMap.entrySet()) {
            out.write((entry.getKey() + "\t" + entry.getValue() + "\n").getBytes());
        }
        out.close();
        fs.close();

        System.out.println("........哈哈..........");
    }

}
