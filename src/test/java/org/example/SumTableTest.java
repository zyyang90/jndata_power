package org.example;

import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;

import static org.junit.Assert.*;

public class SumTableTest {

    @Test
    public void pointString() {
        SumTable sumTable = new SumTable();
        sumTable.setData_time(Timestamp.valueOf("2025-04-01 12:12:12.123"));
        sumTable.setData_value_float(3.14f);

        String expect = "(2025-04-01 12:12:12.123, 3.140000)";
        assertEquals(expect, sumTable.pointString());
    }
}