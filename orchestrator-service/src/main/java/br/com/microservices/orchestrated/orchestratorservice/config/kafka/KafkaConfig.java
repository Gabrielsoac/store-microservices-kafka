package br.com.microservices.orchestrated.orchestratorservice.config.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import com.fasterxml.jackson.databind.ser.std.StringSerializer;

import lombok.RequiredArgsConstructor;

@Configuration
@EnableKafka
@RequiredArgsConstructor
public class KafkaConfig {

    @Value("${string.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${string.kafka.consumer.group-id}")
    private String groupId;
    
    @Value("${string.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProperties());
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        
        return new DefaultKafkaProducerFactory<>(producerProperties());    
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory){
        return new KafkaTemplate<>(producerFactory);
    }

    private Map<String, Object> producerProperties() {
        
        var props = new HashMap<String, Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  StringSerializer.class);
        return props;
    }

    private Map<String, Object> consumerProperties() {
        
        var props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  autoOffsetReset);
        return props;
    }
}
