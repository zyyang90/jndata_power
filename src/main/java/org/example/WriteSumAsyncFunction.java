package org.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class WriteSumAsyncFunction extends RichMapFunction<CdcProcessResult, String> {
    private static final Logger log = LoggerFactory.getLogger(WriteSumAsyncFunction.class);

    private static final int MAX_RETRY = 3;

    // 状态
    private transient ValueState<CdcTable> lastCdcState;

    private final Map<String, String> connParams;
    private transient HikariDataSource ds;
    private transient Connection conn;

    public WriteSumAsyncFunction(Map<String, String> connParams) {
        this.connParams = connParams;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        this.ds = buildDataSource(connParams);
        this.conn = this.ds.getConnection();

        // 初始化 lastCdcState
        ValueStateDescriptor<CdcTable> cdcDesc = new ValueStateDescriptor<>("lastCdc", CdcTable.class);
        lastCdcState = getRuntimeContext().getState(cdcDesc);
    }

    @Override
    public String map(CdcProcessResult cdcProcessResult) throws Exception {
        // factor list
        List<FactorMeta> factorList = cdcProcessResult.getFactor();

        // current cdc
        CdcTable currentCdc = cdcProcessResult.getCurrentCdc();

        // last cdc
        CdcTable lastCdc = lastCdcState.value();
        if (lastCdc == null) {
            lastCdc = query_last_cdc(currentCdc.getTbname());
        }

        // 计算所有的 current sum，并写到 TDengine
        for (FactorMeta factor : factorList) {
            SumTable lastSum = query_last_sum(factor);
            log.info("last sum: {}, current cdc: {}, last cdc: {}", lastSum.pointString(), currentCdc.pointString(),
                    lastCdc.pointString());

            if (Math.abs(currentCdc.getData_value_float() - lastCdc.getData_value_float()) < 1e-6) {
                continue;
            }

            // 计算 sum
            SumTable newSum = calculate_sum(lastSum, currentCdc, lastCdc, factor);
            log.info("current sum: {}", newSum.pointString());

            // 写到 sum 表
            String sql = update_sum_sql(newSum);
            log.info("update sum sql: {}", sql);
            try {
                executeWithRetry(sql);
            } catch (Exception e) {
                log.error("execute sql error: {}", e.getMessage());
            }
        }

        // 更新Flink状态
        lastCdcState.update(currentCdc);

        return "success";
    }

    private CdcTable query_last_cdc(String tableName) {
        CdcTable lastCdc;

        String sql = CdcTable.select_second_last_row_sql(tableName);
        log.debug("query second last cdc: {}", sql);

        try (ResultSet rs = executeQueryWithRetry(sql)) {
            if (rs.next()) {
                lastCdc = CdcTable.try_from(rs);
            } else {
                lastCdc = new CdcTable();
                lastCdc.setTbname(tableName);
                lastCdc.setData_value_float(0);
            }
        } catch (SQLException e) {
            log.error("query last cdc error: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        return lastCdc;
    }

    private SumTable query_last_sum(FactorMeta factor) {
        SumTable last_sum;
        String sql = SumTable.select_last_row_sql(factor.getSum_kks());
        log.debug("query last sum: {}", sql);
        try (ResultSet rs = executeQueryWithRetry(sql)) {
            if (rs.next()) {
                last_sum = SumTable.try_from(rs);
            } else {
                last_sum = SumTable.init(factor);
            }
        } catch (Exception e) {
            log.error("query last sum error: {}", e.getMessage());
            throw new RuntimeException(e);
        }
        return last_sum;
    }

    /// 计算 sum(t1) = sum(t0) + (value(t1) - value(t0)) * factor
    public static SumTable calculate_sum(SumTable lastSum, CdcTable currentCdc, CdcTable lastCdc, FactorMeta factor) {
        float last_sum = lastSum.getData_value_float();
        float cur_cdc = currentCdc.getData_value_float();
        float last_cdc = lastCdc.getData_value_float();
        float fact = factor.getCdc_fact();

        SumTable sum = new SumTable(lastSum);
        float cur_sum = last_sum + (cur_cdc - last_cdc) * fact;
        sum.setData_value_float(cur_sum);

        //System.out.println(cur_sum + " = " + last_sum + " + (" + cur_cdc + " - " + last_cdc + ") * " + fact);
        log.info("calculate sum: {} = {} + ({} - {}) * {}", cur_sum, last_sum, cur_cdc, last_cdc, fact);
        return sum;
    }

    /// 写入 TDengine 的 SQL 语句：
    /// INSERT INTO `jndata`.`{tbname}`
    /// USING `jndata`.`stb_jndata_realtime`
    /// (`kks_no`,`point_factor`,`point_desc`,`point_unit`)
    /// TAGS({tbname},{point_factor},{point_desc},{point_unit})
    /// VALUES({now}, {unit_type}, {data_type}, {data_value_type},{data_value_string},{data_value_float},{
    /// data_value_int}, {data_desc}, {c_method}, {create_time})
    public static String update_sum_sql(SumTable row) {
        return "INSERT INTO `jndata`.`" + row.getKks_no() + "` " +
                // using stable
                "USING `jndata`.`stb_jndata_realtime` " +
                // tags
                "(`kks_no`,`point_factor`,`point_desc`,`point_unit`) " + "TAGS(" +
                (row.getKks_no() == null ? "NULL" : "'" + row.getKks_no() + "'") + "," + row.getPoint_factor() + "," +
                (row.getPoint_desc() == null ? "NULL" : "'" + row.getPoint_desc() + "'") + "," +
                (row.getPoint_unit() == null ? "NULL" : "'" + row.getPoint_unit() + "'") + ") " +
                // values
                "VALUES(now," + (row.getUnit_type() == null ? "NULL" : "'" + row.getUnit_type() + "'") + "," +
                (row.getData_type() == null ? "NULL" : row.getData_type()) + ",NULL," + row.getData_value_float() +
                ",0," + (row.getData_desc() == null ? "NULL" : "'" + row.getData_desc() + "'") + ",NULL,NULL)";
    }

    private ResultSet executeQueryWithRetry(String sql) throws SQLException {
        int retry = 0;
        SQLException exception = null;
        while (retry < MAX_RETRY) {
            try (Statement stmt = conn.createStatement()) {
                return stmt.executeQuery(sql);
            } catch (SQLException e) {
                exception = e;
                log.error("execute query error: {}", e.getMessage());

                try {
                    Thread.sleep(1000L * (1L << retry));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted during retry", e);
                }

                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ex) {
                        log.error("close connection error: {}", ex.getMessage());
                    }
                }
                this.conn = ds.getConnection();
                retry++;
            }
        }
        throw exception;
    }

    private boolean executeWithRetry(String sql) throws SQLException {
        int retry = 0;
        SQLException exception = null;
        while (retry < MAX_RETRY) {
            try (Statement stmt = conn.createStatement()) {
                return stmt.execute(sql);
            } catch (SQLException e) {
                exception = e;
                log.error("execute error: {}", e.getMessage());

                try {
                    Thread.sleep(1000L * (1L << retry));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted during retry", e);
                }

                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ex) {
                        log.error("close connection error: {}", ex.getMessage());
                    }
                }
                this.conn = ds.getConnection();
                retry++;
            }
        }
        throw exception;
    }

    /// 创建 TDengine 连接
    private static HikariDataSource buildDataSource(Map<String, String> map) throws Exception {
        String url = map.get("taos_url");
        Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
        Class.forName("com.taosdata.jdbc.TSDBDriver");

        if (url == null) {
            url = "jdbc:TAOS-RS://localhost:6041/?user=root&password=taosdata";
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setMinimumIdle(10);
        config.setMaximumPoolSize(10);
        config.setConnectionTimeout(30000);
        config.setMaxLifetime(0);
        config.setIdleTimeout(0);
        config.setConnectionTestQuery("SELECT SERVER_VERSION()");

        return new HikariDataSource(config); // create datasource
    }

}
