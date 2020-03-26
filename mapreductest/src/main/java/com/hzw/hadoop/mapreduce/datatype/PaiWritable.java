package com.hzw.hadoop.mapreduce.datatype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Writable;

/**
 * @author hzw
 * @date 2020/3/26  4:22 PM
 * @Description:  自定义数据类型
 */
public class PaiWritable implements Writable {

  private int id;
  private String name;

  public PaiWritable() {
  }

  public PaiWritable(int id, String name) {
    this.set(id,name);
  }

  public void set(int id, String name) {
    this.id = id;
    this.name = name;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PaiWritable that = (PaiWritable) o;
    return id == that.id &&
        Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name);
  }

  @Override
  public String toString() {
    return id + "\t" + name ;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(id);
    dataOutput.writeUTF(name);
  }

  /**
   * 注意字段的写入顺序和读取顺序一定保持一致
   * @param dataInput
   * @throws IOException
   */
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.id = dataInput.readInt();
    this.name = dataInput.readUTF();
  }
}
