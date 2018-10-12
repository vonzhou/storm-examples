package com.vonzhou.examples.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class PropertiesUtils {
    public static String getRedisProp(String key) {
        String fileName = "redis.properties";
        return getPropertyValue(fileName, key);
    }


    public static String getPropertyValue(String fileName, String key) {
        String value = StringUtils.EMPTY;
        Properties pro = new Properties();
        InputStream in = null;
        try {
            in = PropertiesUtils.class.getClassLoader().getResourceAsStream(fileName);
            pro.load(in);
            value = pro.getProperty(key);
        } catch (Exception e) {
            log.error(String.format("提取%s属性值异常!", key), e);
        } finally {
            IOUtils.closeQuietly(in);
        }
        return value != null ? value.trim() : StringUtils.EMPTY;
    }

    public static String getKafkaProp(String key) {
        String fileName = "kafka.properties";
        return getPropertyValue(fileName, key);
    }
}
