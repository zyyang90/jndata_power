package org.example;

import lombok.*;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class FactorMeta implements Serializable {
    /// SUM表的KKS_NO
    private String sum_kks;
    /// SUM表的point_desc
    private String sum_point_desc;
    /// SUM表的point_unit
    private String sum_point_unit;
    /// SUM表的sum_init_value
    private float sum_init_value;
    /// CDC表的KKS_NO
    private String cdc_kks;
    /// CDC表的factor
    private float cdc_fact;

    public static String query_factor_sql() {
        List<String> columns = Arrays.asList("ACCTOTAL_TB.KKS_NO AS sum_kks",
                "ACCTOTAL_TB.\"DESC\" AS " + "sum_point_desc", "ACCTOTAL_TB.UNIT AS sum_point_unit",
                "ACCTOTAL_TB.TOTAL_VALUE AS sum_init_value", "SUBTOTAL_TB.KKS_NO AS cdc_kks",
                "SUBTOTAL_TB.FACT AS cdc_fact");

        String sql = String.join(",", columns);

        return "SELECT " + sql + " FROM ACCTOTAL_TB JOIN SUBTOTAL_TB ON ACCTOTAL_TB.SUB_INDEX = SUBTOTAL_TB.ACC_INDEX;";
    }

    public static FactorMeta try_from(ResultSet rs) throws SQLException {
        FactorMeta meta = new FactorMeta();
        meta.sum_kks = rs.getString("sum_kks");
        meta.sum_point_desc = rs.getString("sum_point_desc");
        meta.sum_point_unit = rs.getString("sum_point_unit");
        meta.sum_init_value = rs.getFloat("sum_init_value");
        meta.cdc_kks = rs.getString("cdc_kks");
        meta.cdc_fact = rs.getFloat("cdc_fact");
        return meta;
    }
}
