package com.ruozedata.mr;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by ruozedata on 2018/10/30.
 */
public class ParamUtils {

    private static Properties properties = new Properties();
    static {
        try {
            properties.load(ParamUtils.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties(){
        return properties;
    }

    public static void main(String[] args) {
        System.out.println(getProperties().getProperty("MAPPER_CLASS"));
    }
}
