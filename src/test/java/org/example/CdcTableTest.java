package org.example;

import org.junit.Test;

import static org.junit.Assert.*;

public class CdcTableTest {

    @Test
    public void select_second_last_row_sql() {
        String tableName = "cdc_table";
        String expected = "SELECT data_time,unit_type,data_type,data_value_string,data_value_float," +
                "data_value_int,data_desc,c_method,create_time,kks_no,point_factor,point_desc,point_unit " +
                "FROM `jndata`.`cdc_table` ORDER BY data_time DESC LIMIT 1,1";
        String actual = CdcTable.select_second_last_row_sql(tableName);
        assertEquals(expected, actual);
    }
}