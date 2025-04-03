package org.example;

import lombok.*;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.stream.Collectors;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class CdcTable implements Serializable {
    // 时间戳
    private Timestamp data_time;
    // 浮点数数据
    private float data_value_float;
    // 整型数据
    private int data_value_int;
    // 静态值，点位的计算系数
    private float point_factor;
    // 子表名/点位编号
    private String tbname;
    // 静态值，点位描述
    private String point_desc;
    // 静态值，点位单位
    private String point_unit;

    public static CdcTable try_from(ResultSet rs) throws SQLException {
        CdcTable cdc = new CdcTable();
        cdc.data_time = rs.getTimestamp("data_time");
        cdc.data_value_float = rs.getFloat("data_value_float");
        cdc.data_value_int = rs.getInt("data_value_int");
        cdc.point_factor = rs.getFloat("point_factor");
        cdc.tbname = rs.getString("kks_no");
        cdc.point_desc = rs.getString("point_desc");
        cdc.point_unit = rs.getString("point_unit");

        return cdc;
    }

    public static String select_second_last_row_sql(String tableName) {
        String cols = String.join(",", SumTable.COLUMNS);
        return "SELECT " + cols + " FROM `jndata`.`" + tableName + "` ORDER BY data_time DESC LIMIT 1,1";
    }

    public String pointString() {
        return "(" + data_time + ", " + data_value_float + ")";
    }
}


