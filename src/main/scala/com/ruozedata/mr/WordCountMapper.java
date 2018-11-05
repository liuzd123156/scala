package com.ruozedata.mr;

/**
 * Created by ruozedata on 2018/10/30.
 */
public class WordCountMapper implements RZMapper {

    public void map(String line, RZContext context) {

        String[] words = line.split("\t");

        for(String word : words) {
            Object value = context.get(word);
            if (value == null) {
                context.write(word, 1);
            } else {
                int v = Integer.parseInt(value.toString());
                context.write(word, v + 1);
            }

        }
    }
}
