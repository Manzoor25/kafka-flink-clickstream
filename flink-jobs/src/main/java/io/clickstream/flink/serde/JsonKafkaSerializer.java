package io.clickstream.flink.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Serializes POJOs as snake_case JSON matching ClickHouse JSONEachRow Kafka Engine tables.
 */
public class JsonKafkaSerializer<T> implements SerializationSchema<T> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper mapper;

    private ObjectMapper mapper() {
        if (mapper == null) {
            mapper = new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        }
        return mapper;
    }

    @Override
    public byte[] serialize(T element) {
        try {
            return mapper().writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize JSON record", e);
        }
    }
}
