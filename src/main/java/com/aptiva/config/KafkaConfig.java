package com.aptiva.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.aptiva.model.Customer;
import com.aptiva.model.GPSData;
import com.aptiva.model.Sales;
import com.aptiva.model.Sales2;

@Configuration
public class KafkaConfig {

	private String groupId;
	@Value("${gps.topic}")
    private String gps_topic;
	@Value("${sales.topic}")
    private String sales_topic;
	@Value("${customer.topic}")
    private String customer_topic;
	@Bean
	 String groupId(@Value("${kafka.csv.group.id}") String groupId) {
		this.groupId = groupId;
		return groupId;
	}

	@Bean
	 ProducerFactory<String, GPSData> producerFactory(){
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1 :9092");
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		DefaultKafkaProducerFactory<String, GPSData> producerFactory = new DefaultKafkaProducerFactory<>(config);
		return producerFactory;
	}

	@Bean
	 ProducerFactory<String, Customer> producerFactoryCustomer(){
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1 :9092");
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		DefaultKafkaProducerFactory<String, Customer> producerFactory = new DefaultKafkaProducerFactory<>(config);
		return producerFactory;
	}
	
	@Bean
	 ProducerFactory<String, Sales2> producerFactorySales2(){
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1 :9092");
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		DefaultKafkaProducerFactory<String, Sales2> producerFactory = new DefaultKafkaProducerFactory<>(config);
		return producerFactory;
	}
	@Bean
	 ProducerFactory<String, Sales> producerFactorySales(){
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1 :9092");
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		DefaultKafkaProducerFactory<String, Sales> producerFactory = new DefaultKafkaProducerFactory<>(config);
		return producerFactory;
	}
	
	@Bean
	 KafkaTemplate<String, GPSData> kafkaGPSTemplate(){
		KafkaTemplate<String, GPSData> kafkaTemplate = new KafkaTemplate<>(producerFactory());
		kafkaTemplate.setDefaultTopic(gps_topic);
		return kafkaTemplate;
	}

	@Bean
	 KafkaTemplate<String, Sales2> kafkaSales2Template(){
		KafkaTemplate<String, Sales2> kafkaTemplate = new KafkaTemplate<>(producerFactorySales2());
		kafkaTemplate.setDefaultTopic(sales_topic);
		return kafkaTemplate;
	}
	
	@Bean
	 KafkaTemplate<String, Sales> kafkaSalesTemplate(){
		KafkaTemplate<String, Sales> kafkaTemplate = new KafkaTemplate<>(producerFactorySales());
		kafkaTemplate.setDefaultTopic(sales_topic);
		return kafkaTemplate;
	}
	
	@Bean
	 KafkaTemplate<String, Customer> kafkaCustomerTemplate(){
		KafkaTemplate<String, Customer> kafkaTemplate = new KafkaTemplate<>(producerFactoryCustomer());
		kafkaTemplate.setDefaultTopic(customer_topic);
		return kafkaTemplate;
	}
	
//	@Bean
//	public ConsumerFactory<String, GPSData> consumerFactory(){
//		Map<String, Object> config = new HashMap<>();
//		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
//		config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserJsonDeserializer.class);
//		return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new UserJsonDeserializer());
//	}
//
//	@Bean
//	public ConcurrentKafkaListenerContainerFactory<String, User> kafkaListenerContainerFactory(){
//		ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
//		factory.setConsumerFactory(consumerFactory());
//		return factory;
//	}



}