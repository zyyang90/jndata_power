package org.example;

import com.taosdata.flink.cdc.TDengineCdcSource;
import com.taosdata.flink.common.TDengineCdcParams;
import com.taosdata.flink.common.TDengineConfigParams;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.flink.streaming.api.CheckpointingMode.AT_LEAST_ONCE;

public class BatchJob {
    private static final Logger log = LoggerFactory.getLogger(BatchJob.class);

    public static void main(String[] args) throws Exception {
        // 初始化 Flink 环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100, AT_LEAST_ONCE);

        ParameterTool tool = ParameterTool.fromArgs(args);
        ExecutionConfig config = env.getConfig();
        // 设置参数
        config.setGlobalJobParameters(tool);
        // 设置重启策略
        config.setRestartStrategy(RestartStrategies.noRestart());

        // 连接 DM 数据库，获取 factor map
        Map<String, String> map = tool.toMap();
        Connection dm_conn = connect_to_dm(map);
        Map<String, List<FactorMeta>> factorMetaMap = load_factor_map(dm_conn);

        // 定义广播流，将 factor map 广播到所有 TaskManager
        MapStateDescriptor<String, List<FactorMeta>> factorDesc = new MapStateDescriptor<>("factorMap", String.class,
                TypeInformation.of(new TypeHint<List<FactorMeta>>() {
                }).getTypeClass());
        BroadcastStream<Map<String, List<FactorMeta>>> factorStream = env.fromData(factorMetaMap).broadcast(factorDesc);

        // 构造 TDengine CDC Source
        TDengineCdcSource<CdcTable> cdcSource = buildCdcSource(env);
        // 接入 CDC Stream
        DataStream<CdcTable> keyedCdcStream = env.fromSource(cdcSource, WatermarkStrategy.noWatermarks(),
                "TDengineCdcSource").keyBy(CdcTable::getTbname);

        // 连接 cdcStream 和 factorStream，处理结果，写入 TDengine
        DataStream<String> resultStream = keyedCdcStream.connect(factorStream)
                                                        .process(new CdcProcessFunction())
                                                        .keyBy(cdcResult -> cdcResult.getCurrentCdc().getTbname())
                                                        .map(new WriteSumAsyncFunction(map));
        resultStream.print();

        String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis());
        env.execute("jndata flink job: " + now);
    }

    /// 构造 CDC Source
    private static TDengineCdcSource<CdcTable> buildCdcSource(StreamExecutionEnvironment env) {
        ExecutionConfig.GlobalJobParameters params = env.getConfig().getGlobalJobParameters();
        Map<String, String> map = params.toMap();
        String group_id = map.get("group.id");
        if (group_id == null) {
            group_id = "group_1";
        }
        String user = map.get(TDengineCdcParams.CONNECT_USER);
        if (user == null) {
            user = "root";
        }
        String password = map.get(TDengineCdcParams.CONNECT_PASS);
        if (password == null) {
            password = "taosdata";
        }

        Properties config = new Properties();
        config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, map.get("bootstrap.servers"));
        config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS, "100");
        config.setProperty(TDengineCdcParams.GROUP_ID, group_id);
        config.setProperty(TDengineCdcParams.ENABLE_AUTO_COMMIT, "true");
        config.setProperty(TDengineCdcParams.CONNECT_USER, user);
        config.setProperty(TDengineCdcParams.CONNECT_PASS, password);
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, CdcTableDeserializer.class.getName());
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
        config.setProperty(TDengineConfigParams.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        config.setProperty(TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        config.setProperty(TDengineConfigParams.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "3000");
        // 订阅 topic：power_topic，获取点位的 current 数据
        return new TDengineCdcSource<CdcTable>(map.get("topic"), config, CdcTable.class);
    }

    /// 从 DM 中查出所有 factorMeta, key: cdc_kks, value: factorMeta
    private static HashMap<String, List<FactorMeta>> load_factor_map(Connection dmConn) {
        HashMap<String, List<FactorMeta>> map = new HashMap<>();

        String sql = FactorMeta.query_factor_sql();
        log.info("query factor from DM: {}", sql);
        try (Statement stmt = dmConn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                FactorMeta factor = FactorMeta.try_from(rs);
                log.info("{}", factor);
                String cdc_kks = factor.getCdc_kks();
                if (map.containsKey(cdc_kks)) {
                    map.get(cdc_kks).add(factor);
                } else {
                    ArrayList<FactorMeta> factorList = new ArrayList<>();
                    factorList.add(factor);
                    map.put(cdc_kks, factorList);
                }
            }
        } catch (SQLException e) {
            log.error("load factor map error: {}", e.getMessage());
            throw new RuntimeException(e);
        } finally {
            if (dmConn != null) {
                try {
                    dmConn.close();
                } catch (SQLException e) {
                    log.error("close dm connection error: {}", e.getMessage());
                }
            }
        }

        return map;
    }

    /// 创建 DM 连接
    private static Connection connect_to_dm(Map<String, String> params) throws Exception {
        Class.forName("dm.jdbc.driver.DmDriver");

        String url = params.get("dm.url");
        if (url == null) {
            throw new Exception("dm.url is not set");
        }
        String username = params.get("dm.user");
        if (username == null) {
            throw new Exception("dm.user is not set");
        }
        String password = params.get("dm.password");
        if (password == null) {
            throw new Exception("dm.password is not set");
        }

        return DriverManager.getConnection(url, username, password);
    }
}
