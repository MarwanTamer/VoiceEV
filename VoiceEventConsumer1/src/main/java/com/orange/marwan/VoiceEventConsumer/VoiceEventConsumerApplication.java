package com.orange.marwan.VoiceEventConsumer;


import com.orange.marwan.VoiceEventConsumer.objects.OEGEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

@SpringBootApplication
public class VoiceEventConsumerApplication {

	public static void main(String[] args) {
		// SpringApplication.run(VoiceEventConsumerApplication.class, args);
		String group = "testgroup";
		ConstantUtls.loadCons();
		final StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties proper = new Properties();
		proper.setProperty("group.id", group);
		proper.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConstantUtls.bootStraperServer);
		proper.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		proper.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		proper.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConstantUtls.bootStraperServer);
		proper.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		proper.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		proper.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		try {
			KafkaSource<OEGEvent> source = KafkaSource.<OEGEvent>builder()
					.setBootstrapServers(ConstantUtls.bootStraperServer)
					.setTopics(ConstantUtls.consumerTopicName).setGroupId("test")
					.setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer((DeserializationSchema<OEGEvent>) new OEGEvent()).build();

			executionEnvironment.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").keyBy(value -> value.aggregationId)
					.reduce(new DataAggregation()).addSink(new FlinkKafkaProducer<OEGEvent>(
					ConstantUtls.producerTopicName, (SerializationSchema<OEGEvent>) new OEGEvent(), proper));
			executionEnvironment.execute("Kafka Source");
		} catch (Exception ex) {

		}
	}

}
