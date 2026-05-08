package com.virinchi.workerservice.config;

import com.virinchi.workerservice.messaging.ChunkMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, ChunkMessage> chunkMessageConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        JsonDeserializer<ChunkMessage> deserializer = new JsonDeserializer<>(ChunkMessage.class);
        deserializer.addTrustedPackages("com.virinchi.workerservice.messaging");
        deserializer.setUseTypeHeaders(false);
        deserializer.setRemoveTypeHeaders(true);
        deserializer.setUseTypeMapperForKey(false);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ChunkMessage> chunkMessageKafkaListenerContainerFactory(
            ConsumerFactory<String, ChunkMessage> chunkMessageConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, ChunkMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(chunkMessageConsumerFactory);
        return factory;
    }
}