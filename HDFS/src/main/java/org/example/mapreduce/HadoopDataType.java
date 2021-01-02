package org.example.mapreduce;

import org.apache.hadoop.io.*;
import org.checkerframework.checker.units.qual.A;

import java.time.Year;

/**
 * （1）BooleanWritable：标准布尔型数值。
 * （2）ByteWritable：单字节数值。
 * （3）DoubleWritable：双字节数。
 * （4）FloatWritable：浮点数。
 * （5）IntWritable：整型数。
 * （6）LongWritable：长整型数。
 * （7）Text：使用UTF8格式存储的文本。
 * （8）NullWritable：当<key,value>中的key或value为空时使用。
 * （9）ArrayWritable：存储属于Writable类型的值数组
 */
public class HadoopDataType {

    public static void main(String[] args) {
        Text text = new Text("Hello World");
        System.out.println(text.getLength());
        System.out.println(text.find("o"));
        System.out.println(text.toString());

        System.out.println("-------------------------------------");

        ArrayWritable arrayWritable = new ArrayWritable(IntWritable.class);
        IntWritable year = new IntWritable(2021);
        IntWritable month = new IntWritable(01);
        IntWritable day = new IntWritable(02);
        arrayWritable.set(new IntWritable[]{year,month,day});
        System.out.println(
                String.format("year=%d,month=%d,day=%d",
                        ((IntWritable)arrayWritable.get()[0]).get(),
                        ((IntWritable)arrayWritable.get()[1]).get(),
                        ((IntWritable)arrayWritable.get()[2]).get()
                        )
        );

        System.out.println("-------------------------------------");

        MapWritable mapWritable = new MapWritable();
        Text k1 = new Text("name");
        Text v1 = new Text("tom");
        Text k2 = new Text("age");
        mapWritable.put(k1,v1);
        mapWritable.put(k2, NullWritable.get());
        System.out.println(mapWritable.get(k1).toString());
        System.out.println(mapWritable.get(k2).toString());

    }
}
