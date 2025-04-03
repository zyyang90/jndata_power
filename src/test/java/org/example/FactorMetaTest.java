package org.example;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class FactorMetaTest {

    @Test
    public void query_factor_sql() {
        String expect = "SELECT ACCTOTAL_TB.KKS_NO AS sum_kks," + "ACCTOTAL_TB.\"DESC\" AS sum_point_desc," +
                "ACCTOTAL_TB.UNIT AS sum_point_unit," + "ACCTOTAL_TB.TOTAL_VALUE AS sum_init_value," +
                "SUBTOTAL_TB.KKS_NO AS cdc_kks," + "SUBTOTAL_TB.FACT AS cdc_fact " +
                "FROM ACCTOTAL_TB JOIN SUBTOTAL_TB ON ACCTOTAL_TB.SUB_INDEX = SUBTOTAL_TB.ACC_INDEX;";
        String sql = FactorMeta.query_factor_sql();
        System.out.println(sql);
        Assert.assertEquals(expect, sql);
    }
}