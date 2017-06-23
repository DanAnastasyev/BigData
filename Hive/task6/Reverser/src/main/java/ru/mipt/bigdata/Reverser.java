package ru.mipt.bigdata;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
        name = "ReverserUDF",
        value = "Returns reversed string"
)
public class Reverser extends UDF {
    public String evaluate(String str) {
        return new StringBuffer(str).reverse().toString();
    }
}
