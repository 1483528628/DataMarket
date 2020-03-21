package cn.itcast.canal.protobuf;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ProtoBufSerializer implements Serializer<ProtoBufable> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ProtoBufable data) {
        return data.toByte();
    }

    @Override
    public void close() {

    }
}
