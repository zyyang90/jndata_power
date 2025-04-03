package org.example;

import lombok.*;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class SumTable implements Serializable {
    /// 时间戳：写 now
    private Timestamp data_time;
    /// 单位，SUM表写NULL
    private String unit_type;
    /// 数据类型，SUM表为NULL，
    private String data_type;
    /// 数据值字符串，SUM表写 NULL
    private String data_value_string;
    /// 浮点数数据，SUM表写sum值
    private float data_value_float;
    /// 整型数据：SUM表写 0
    private int data_value_int;
    /// 数据描述：SUM表写NULL
    private String data_desc;
    /// c_method：SUM表写NULL
    private String c_method;
    /// 创建时间，SUM表写NULL
    private Timestamp create_time;
    /// 子表名/点位编号，TAG，SUM表是ACCTOTAL.KKS_NO, CDC表是 SUBTOTAL.KKS_NO
    private String kks_no;
    /// 点位的计算系数，TAG, SUM表写NULL，CDC表是SUBTOTAL.FACTOR
    private float point_factor;
    /// 点位描述，TAG，SUM表是ACCTOTAL.DESC, CDC表是SUBTOTAL.DESC
    private String point_desc;
    /// 点位单位，TAG,SUM表是ACCTOTAL.UNIT, CDC表是SUBTOTAL.UNIT
    private String point_unit;

    public SumTable(SumTable sum) {
        this.data_time = sum.data_time;
        this.unit_type = sum.unit_type;
        this.data_type = sum.data_type;
        this.data_value_string = sum.data_value_string;
        this.data_value_float = sum.data_value_float;
        this.data_value_int = sum.data_value_int;
        this.data_desc = sum.data_desc;
        this.c_method = sum.c_method;
        this.create_time = sum.create_time;
        this.kks_no = sum.kks_no;
        this.point_factor = sum.point_factor;
        this.point_desc = sum.point_desc;
        this.point_unit = sum.point_unit;
    }

    public static List<String> COLUMNS = Arrays.asList("data_time", "unit_type", "data_type", "data_value_string",
            "data_value_float", "data_value_int", "data_desc", "c_method", "create_time", "kks_no", "point_factor",
            "point_desc", "point_unit");

    /// SELECT last_row(data_time) as `data_time`, ...
    /// FROM `jndata`.`stb_jndata_realtime`
    /// WHERE tbname = '{tbname}'
    public static String select_last_row_sql(String tbname) {
        String cols = COLUMNS.stream()
                             .map(col -> "last_row(" + col + ") as `" + col + "`")
                             .collect(Collectors.joining(","));
        return "SELECT " + cols + " FROM `jndata`.`stb_jndata_realtime` WHERE tbname = '" + tbname + "'";
    }

    public static SumTable try_from(ResultSet rs) throws SQLException {
        SumTable sum = new SumTable();
        sum.data_time = rs.getTimestamp("data_time");
        sum.unit_type = rs.getString("unit_type");
        sum.data_type = rs.getString("data_type");
        sum.data_value_string = rs.getString("data_value_string");
        sum.data_value_float = rs.getFloat("data_value_float");
        sum.data_value_int = rs.getInt("data_value_int");
        sum.data_desc = rs.getString("data_desc");
        sum.c_method = rs.getString("c_method");
        sum.create_time = rs.getTimestamp("create_time");
        sum.kks_no = rs.getString("kks_no");
        sum.point_factor = rs.getFloat("point_factor");
        sum.point_desc = rs.getString("point_desc");
        sum.point_unit = rs.getString("point_unit");
        return sum;
    }

    /// SUM表的初始值
    public static SumTable init(FactorMeta factorMeta) {
        SumTable sum = new SumTable();
        // sum表的 kks_no == ACCTOTAL.KKS_NO
        sum.kks_no = factorMeta.getSum_kks();
        // sum表的 data_value_float初始值 == ACCTOTAL.TOTAL_VALUE
        sum.data_value_float = factorMeta.getSum_init_value();
        // sum表的 point_desc == ACCTOTAL.DESC
        sum.point_desc = factorMeta.getSum_point_desc();
        // sum表的 point_unit == ACCTOTAL.UNIT
        sum.point_unit = factorMeta.getSum_point_unit();
        return sum;
    }

    public String pointString() {
        return String.format("(%s, %f)", data_time, data_value_float);
    }
}
