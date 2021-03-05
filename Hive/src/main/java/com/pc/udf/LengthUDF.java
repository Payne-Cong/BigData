package com.pc.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 自定义UDF 取字符串长度
 * hive: add jar '<path>'
 * create temporary function my_length as 'com.pc.udf.LengthUDF'
 * select my_length("abc"); ==>3 正常使用
 */
public class LengthUDF extends GenericUDF {


    // 初始化 校验参数
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        if(objectInspectors.length != 1){
            throw new UDFArgumentException("入参数量不为1");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    // 计算
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        // deferredObjects 内部还有包装类型
        String arg = deferredObjects[0].get().toString();
        if(arg == null) return 0;
        return arg.length();
    }

    /**
     *  explain 阶段显示
     * @param strings
     * @return
     */
    public String getDisplayString(String[] strings) {
        return "";
    }
}
