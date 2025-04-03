package org.example;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class WriteSumAsyncFunctionTest {

    @Test
    public void update_sum_sql() {
        FactorMeta factor = new FactorMeta();
        factor.setSum_kks("sum_kks");
        factor.setCdc_kks("cdc_kks");
        factor.setCdc_fact(1.0f);
        factor.setSum_point_desc("描述");
        factor.setSum_point_unit("单位");

        // sum = 0
        SumTable sum0 = SumTable.init(factor);
        // cdc = 0
        CdcTable cdc0 = new CdcTable();

        // cdc = 10
        CdcTable cdc1 = new CdcTable();
        cdc1.setData_value_float(10.0f);

        // sum = 10 = 0 + (10 - 0) * 1
        SumTable sum1 = WriteSumAsyncFunction.calculate_sum(sum0, cdc1, cdc0, factor);
        Assert.assertEquals(10.0f, sum1.getData_value_float(), 0.01f);

        // cdc = 20
        CdcTable cdc2 = new CdcTable();
        cdc2.setData_value_float(20.0f);

        // sum = 20 = 10 + (20 - 10) * 1
        SumTable sum2 = WriteSumAsyncFunction.calculate_sum(sum1, cdc2, cdc1, factor);
        Assert.assertEquals(20.0f, sum2.getData_value_float(), 0.01f);
    }

}