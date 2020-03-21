package com.itcast.canal_client.kafka;

import cn.itcast.canal.bean.RowData;
import com.itcast.canal_client.util.ConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaSender {
    private KafkaProducer<String, RowData> kafkaProducer;
    private Properties kafkaProps = new Properties();
    public KafkaSender() {
        kafkaProps.put("bootstrap.servers", ConfigUtil.kafkaBootstrap_servers_config());
        kafkaProps.put("acks", ConfigUtil.kafkaAcks());
        kafkaProps.put("retries", ConfigUtil.kafkaRetries());
        kafkaProps.put("batch.size", ConfigUtil.kafkaBatch_size_config());
        kafkaProps.put("key.serializer", ConfigUtil.kafkaKey_serializer_class_config());
        kafkaProps.put("value.serializer", ConfigUtil.kafkaValue_serializer_class_config());
        kafkaProducer = new KafkaProducer<String, RowData>(kafkaProps);
    }

    public void send(RowData rowData) {
        kafkaProducer.send(new ProducerRecord<>(ConfigUtil.kafkaTopic(),null,rowData));
    }
}
