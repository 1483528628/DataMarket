package com.itheima.canal_demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;

public class CanalClientEntrance {
    public static void main(String[] args) {
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("192.168.100.201", 11111),
                "example", "canal", "canal"
        );
        int batchSize = 5 * 1024;
        boolean flag = true;
        try {
            connector.connect();
            connector.rollback();
            connector.subscribe("test.*");
            while (flag) {
                Message message = connector.getWithoutAck(batchSize);
//                获取batchId
                long batchId = message.getId();
//                获取binlog数据的条数
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {

                } else {
//                    printSummary(message);
//                    String s = binlogToJson(message);
//                    System.out.println(s);
                    binlogToProtobuf(message);
                }
//                确认指定的batchId已经消费成功
                connector.ack(batchId);
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        } finally {
            connector.disconnect();
        }

    }

    private static byte[] binlogToProtobuf(Message message) throws InvalidProtocolBufferException {
        CanalModel.RowData.Builder rowDataBuilder = CanalModel.RowData.newBuilder();
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

            rowDataBuilder.setLogfilename(logfileName);
            rowDataBuilder.setLogfileoffset(logfileOffset);
            rowDataBuilder.setExecuteTime(executeTime);
            rowDataBuilder.setSchemaName(schemaName);
            rowDataBuilder.setTableName(tableName);
            rowDataBuilder.setEventType(eventType);
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                if (eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        rowDataBuilder.putColumns(column.getName(), column.getValue().toString());
                    }
                } else if (eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        rowDataBuilder.putColumns(column.getName(), column.getValue().toString());
                    }
                }
            }
        }
        return rowDataBuilder.build().toByteArray();
    }

    private static String binlogToJson(Message message) throws InvalidProtocolBufferException {
        HashMap<String, Object> rowDataMap = new HashMap<String, Object>();
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
            HashMap<String, Object> columnDataMap = new HashMap<String, Object>();
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> columnDataList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : columnDataList) {
                if (eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue());
                    }
                } else if (eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue());
                    }
                }
            }
            rowDataMap.put("columns", columnDataMap);
        }
        return JSON.toJSONString(rowDataMap);
    }

    private static void printSummary(Message message) {
//        遍历整个batch中的每个binlig实体
        for (CanalEntry.Entry entry : message.getEntries()) {
//            事务开启
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
//            获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
//            获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
//            获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
//            获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
//            获取表名
            String tableName = entry.getHeader().getTableName();
//            获取事件类型
            String eventTypeName = entry.getHeader().getEventType().toString().toLowerCase();
//            System.out.println("logfileName" + ":" + logfileName);
//            System.out.println("logfileOffset" + ":" + logfileOffset);
//            System.out.println("executeTime" + ":" + executeTime);
//            System.out.println("schemaName" + ":" + schemaName);
//            System.out.println("tableName" + ":" + tableName);
//            System.out.println("eventTypeName" + ":" + eventTypeName);
            CanalEntry.RowChange rowChange = null;
            try {
//                获取存储数据，并将二进制字节数据解析为RowChange实体
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                if (entry.getHeader().getEventType() == CanalEntry.EventType.DELETE) {
                    System.out.println("- - -delete- - -");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("- - -");
                } else if (entry.getHeader().getEventType() == CanalEntry.EventType.UPDATE) {
                    System.out.println("- - -update- - -");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("- - -");
                    printColumnList(rowData.getAfterColumnsList());
                } else if (entry.getHeader().getEventType() == CanalEntry.EventType.INSERT) {
                    System.out.println("- - -insert- - -");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("- - -");
                    printColumnList(rowData.getAfterColumnsList());

                }
            }
        }

    }

    private static void printColumnList(List<CanalEntry.Column> columnsList) {
        for (CanalEntry.Column column : columnsList) {
            System.out.println(column.getName() + "\t" + column.getValue());
        }
    }
}
