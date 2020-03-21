package cn.itcast.canal.bean;

import cn.itcast.canal.protobuf.CanalModel;
import cn.itcast.canal.protobuf.ProtoBufable;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.HashMap;
import java.util.Map;

public class RowData implements ProtoBufable {
    private String logfilename;
    private Long logfileoffset;
    private Long executeTime;
    private String schemaName;
    private String tableName;
    private String eventType;
    private Map<String, String> columns;

    public RowData() {
    }

    public RowData(Map map) {
        this.logfilename = map.get("logfileName").toString();
        this.logfileoffset = Long.parseLong(map.get("logfileOffset").toString());
        this.executeTime = Long.parseLong(map.get("executeTime").toString());
        this.schemaName = map.get("schemaName").toString();
        this.tableName = map.get("tableName").toString();
        this.eventType = map.get("eventType").toString();
        this.columns = (Map<String, String>)map.get("columns");
    }

    public RowData(String logfilename,
                   Long logfileoffset,
                   Long executeTime,
                   String schemaName,
                   String tableName,
                   String eventType,
                   Map<String, String> columns) {
        this.logfilename = logfilename;
        this.logfileoffset = logfileoffset;
        this.executeTime = executeTime;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.eventType = eventType;
        this.columns = columns;
    }

    public RowData(byte[] bytes) {
        try {
            CanalModel.RowData rowData = CanalModel.RowData.parseFrom(bytes);
            this.logfilename = rowData.getLogfileName();
            this.logfileoffset = rowData.getLogfileOffset();
            this.executeTime = rowData.getExecuteTime();
            this.tableName = rowData.getTableName();
            this.eventType = rowData.getEventType();
            // 将所有map列值添加到可变HashMap中
            this.columns = new HashMap<>();
            columns.putAll(rowData.getColumnsMap());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public byte[] toByte() {
        CanalModel.RowData.Builder builder = CanalModel.RowData.newBuilder();
        builder.setLogfileName(this.logfilename);
        builder.setLogfileOffset(this.logfileoffset);
        builder.setExecuteTime(this.executeTime);
        builder.setTableName(this.tableName);
        builder.setEventType(this.eventType);
        for (String key : this.columns.keySet()) {
            builder.putColumns(key, this.columns.get(key));
        }
        return builder.build().toByteArray();
    }

    @Override
    public String toString() {
        return "RowData{" +
                "logfilename='" + logfilename + '\'' +
                ", logfileoffset=" + logfileoffset +
                ", executeTime=" + executeTime +
                ", schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", eventType='" + eventType + '\'' +
                ", columns=" + columns +
                '}';
    }
}
