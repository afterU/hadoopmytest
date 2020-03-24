package com.hzw.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * @author hzw
 * @date 2020/3/1  6:50 PM
 * @Description:
 */
public class HdfsApp {




  public static void main(String[] args) throws IOException {



  }

  /**
   * 写文件
   * @throws Exception
   */
  private static void writeFile()throws Exception{
    //远程javaAPI操作HDFS的写操作没有权限，需要模拟root用户
    Properties properties = System.getProperties();
    properties.setProperty("HADOOP_USER_NAME","root");

    FileSystem fileSystemClient = getFileSystemClient();

    String targetFile = "/user/hzw/mapreduce/wordcount/input/put-wc.input";
    Path targetPath = new Path(targetFile);

    FSDataOutputStream outputStream = fileSystemClient.create(targetPath);

    FileInputStream inputStream = new FileInputStream(
        new File("/Users/huangzhiwei/myTestProject/hadoopmytest/hdfstest/src/main/resources/log4j.properties"));

    try {
      IOUtils.copyBytes(inputStream,outputStream,4096,false);
    }catch (Exception e){
      e.printStackTrace();
    }finally {
      IOUtils.closeStream(inputStream);
      IOUtils.closeStream(outputStream);
    }
  }
  /**
   * 读取文件
   * @throws IOException
   */
  private static void readFile() throws IOException {
    FileSystem fileSystem = getFileSystemClient();

    String filePath = "/user/hzw/mapreduce/wordcount/input/wc.input";

    Path file = new Path(filePath);

    FSDataInputStream inputStream = fileSystem.open(file);

    try {
      IOUtils.copyBytes(inputStream,System.out,4096,false);
    }catch (Exception e){
      e.printStackTrace();
    }finally {
      IOUtils.closeStream(inputStream);
    }
  }


  /**
   * 获取文件系统客户端
   * @return
   * @throws IOException
   */
  private static FileSystem getFileSystemClient() throws IOException {

    Configuration configuration = new Configuration();

   return  FileSystem.get(configuration);
  }

}
