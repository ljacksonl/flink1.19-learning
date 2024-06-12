package cn.ljs.sql.model;

import org.apache.flink.table.annotation.DataTypeHint;

import java.math.BigDecimal;

public class User {

    // 1. 基础类型，Flink 可以通过反射类型信息自动把数据类型获取到
    // 关于 SQL 类型和 Java 类型之间的映射见：https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/types/#data-type-extraction
    public int age;
    public String name;

    // 2. 复杂类型，用户可以通过 @DataTypeHint("DECIMAL(10, 2)") 注解标注此字段的数据类型
    public @DataTypeHint("DECIMAL(10, 2)") BigDecimal totalBalance;
}