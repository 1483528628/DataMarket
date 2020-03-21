package com.itcast.canal_client;

import cn.itcast.canal.bean.RowData;
import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.itcast.canal_client.kafka.KafkaSender;
import com.itcast.canal_client.util.ConfigUtil;

import java.util.HashMap;
import java.util.Map;

public class CanalClient {
    private static CanalConnector canalConnector;
    private static KafkaSender kafkaSender;
    private static final int BATCH_SIZE = 5 * 1024;
    public CanalClient() {
        canalConnector= CanalConnectors.newClusterConnector(
                ConfigUtil.zookeeperServerIp(),
                ConfigUtil.canalServerDestination(),
                ConfigUtil.canalServerUsername(),
                ConfigUtil.canalServerPassword()
        );
         kafkaSender = new KafkaSender();
    }

    public void start() {
        try {
            while (true) {
                canalConnector.connect();
                canalConnector.rollback();
                canalConnector.subscribe(ConfigUtil.canalSubscribeFilter());
                while (true) {
                    Message message = canalConnector.getWithoutAck(BATCH_SIZE);
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {

                    } else {
                        Map binlogMsgMap = binlogMessage2Map(message);
                        RowData rowData = new RowData(binlogMsgMap);
//                        System.out.println(rowData.toString());
//                        System.out.println("---");
//                        System.out.println(JSON.toJSON(rowData));
                        kafkaSender.send(rowData);
                    }
                    canalConnector.ack(batchId);
                }
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        } finally {
            canalConnector.disconnect();
        }
    }

    private Map binlogMessage2Map(Message message) throws InvalidProtocolBufferException {
        HashMap<String, Object> rowDataMap = new HashMap<>();
        for (CanalEntry.Entry entry : message.getEntries()) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            // 获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
            // 获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
            // 获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取事件类型 insert/update/delete
            String eventType = entry.getHeader().getEventType().toString().toLowerCase();
            rowDataMap.put("logfileName", logfileName);
            rowDataMap.put("logfileOffset", logfileOffset);
            rowDataMap.put("executeTime", executeTime);
            rowDataMap.put("schemaName", schemaName);
            rowDataMap.put("tableName", tableName);
            rowDataMap.put("eventType", eventType);
            HashMap<String, String> columnDataMap = new HashMap<>();
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                if (eventType == "insert" || eventType == "update") {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue().toString());
                    }
                } else if (eventType == "delete") {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue().toString());
                    }
                }
            }
            rowDataMap.put("columns", columnDataMap);
        }
        return rowDataMap;
    }
}
