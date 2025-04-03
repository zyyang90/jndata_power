package org.example;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CdcProcessFunction extends KeyedBroadcastProcessFunction<String, CdcTable, Map<String, List<FactorMeta>>, CdcProcessResult> {
    private static final Logger log = LoggerFactory.getLogger(CdcProcessFunction.class);

    // 状态描述
    private transient MapStateDescriptor<String, List<FactorMeta>> factorDesc;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        // 初始化 factorDesc
        factorDesc = new MapStateDescriptor<>("factorMap", String.class,
                TypeInformation.of(new TypeHint<List<FactorMeta>>() {
                }).getTypeClass());
    }

    @Override
    public void processBroadcastElement(Map<String, List<FactorMeta>> factorMap,
            KeyedBroadcastProcessFunction<String, CdcTable, Map<String, List<FactorMeta>>, CdcProcessResult>.Context ctx,
            Collector<CdcProcessResult> out) throws Exception {
        ctx.getBroadcastState(factorDesc).putAll(factorMap);
    }

    @Override
    public void processElement(CdcTable currentCdc,
            KeyedBroadcastProcessFunction<String, CdcTable, Map<String, List<FactorMeta>>, CdcProcessResult>.ReadOnlyContext ctx,
            Collector<CdcProcessResult> out) throws Exception {

        // 获取广播的 factorMap
        ReadOnlyBroadcastState<String, List<FactorMeta>> broadcastState = ctx.getBroadcastState(factorDesc);

        String tbname = currentCdc.getTbname();
        if (tbname == null) {
            log.error("tbname is null, current cdc: {}", currentCdc);
            throw new RuntimeException("tbname is null, current cdc: " + currentCdc);
        }

        List<FactorMeta> factorList = broadcastState.get(tbname);
        if (factorList == null || factorList.isEmpty()) {
            log.error("factor not found for KKS_NO: {}", currentCdc.getTbname());
            return; // 直接返回，不处理当前记录
        }

        // 封装数据，传递给异步 IO
        CdcProcessResult result = new CdcProcessResult(currentCdc, factorList);
        out.collect(result);
    }

}
