package com.alone.kafka.utils;

import java.io.InputStream;
import java.util.Properties;

/**
*@Param:
*@Author: liyang
*@date: 2022-01-06
*@return:
*@Description: 统一读取配置
*/
public class ConfigurationManager {

  private static Properties prop = new Properties();

  static {
    try {
      InputStream inputStream = ConfigurationManager.class.getClassLoader()
          .getResourceAsStream("db.properties");
      prop.load(inputStream);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  //获取配置项
  public static String getProperty(String key) {
    return prop.getProperty(key);
  }

  //获取布尔类型的配置项
  public static boolean getBoolean(String key) {
    String value = prop.getProperty(key);
    try {
      return Boolean.valueOf(value);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

}
