package com.ruozedata.mr;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ruozedata on 2018/10/30.
 */
public class RZContext {

    private Map<Object,Object> cacheMap = new HashMap<Object, Object>();

    public Map<Object,Object> getCacheMap() {
        return  cacheMap;
    }

    public void write(Object key, Object value) {
        cacheMap.put(key,value);
    }

    public Object get(Object key) {
        return cacheMap.get(key);
    }


}
