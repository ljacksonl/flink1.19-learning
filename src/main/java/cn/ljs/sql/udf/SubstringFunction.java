package cn.ljs.sql.udf;

import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.*;

// 定义一个带有输入参数的标量函数
public class SubstringFunction extends ScalarFunction {

    //boolean 默认就是 Serializable 的
    private boolean endInclusive;

    public SubstringFunction(boolean endInclusive) {
        this.endInclusive = endInclusive;
    }

    public String eval(String s, Integer begin, Integer end) {
        return s.substring(begin, endInclusive ? end + 1 : end);
    }

    public static void main(String[] args) {
//        TableEnvironment env = TableEnvironment.create();
//
//        // Table API 调用 UDF
//        env.from("MyTable").select(call(new SubstringFunction(true), $("myField"), 5, 12));
//
//        // 注册 UDF
//        env.createTemporarySystemFunction("SubstringFunction", new SubstringFunction(true));
    }
}

