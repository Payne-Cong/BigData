package com.pc.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


import java.util.ArrayList;
import java.util.List;

/**
 * UDTF : 一行 转 多行
 * 例 : a,b,c
 * ========>
 * a
 * b
 * c
 *
 */
public class MyUDTF extends GenericUDTF {

    //输出数据的集合
    List<String> outData = new ArrayList<String>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        // 输出数据的默认列名
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("col_word1");

        // 输出数据的类型
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // a,b,c
    public void process(Object[] args) throws HiveException {

        String[] words = args[0].toString().split(",");

        for (String word : words) {
            outData.clear();

            outData.add(word);
            //输出
            forward(outData);

        }


    }

    public void close() throws HiveException {

    }
}
