package io.clickstream.flink.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clickstream.flink.model.ClickEvent;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Deserializes Confluent Schema Registry wire-framed Avro payloads ([0x00][4B schemaId][datum])
 * into ClickEvent POJOs, with transparent JSON fallback for fixture tests.
 */
public class ConfluentAvroClickDeserializer implements DeserializationSchema<ClickEvent> {
    private static final long serialVersionUID = 1L;
    private static final byte CONFLUENT_MAGIC_BYTE = 0x00;

    private transient GenericDatumReader<GenericRecord> datumReader;
    private transient BinaryDecoder decoder;
    private transient ObjectMapper jsonMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        initIfNeeded();
    }

    private void initIfNeeded() throws IOException {
        if (datumReader == null) {
            try (InputStream in = getClass().getResourceAsStream("/schemas/click_event.avsc")) {
                if (in == null) {
                    throw new IOException("Missing classpath resource /schemas/click_event.avsc");
                }
                Schema schema = new Schema.Parser().parse(in);
                this.datumReader = new GenericDatumReader<>(schema);
            }
        }
        if (jsonMapper == null) {
            this.jsonMapper = new ObjectMapper();
        }
    }

    @Override
    public ClickEvent deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        initIfNeeded();

        if (message[0] == CONFLUENT_MAGIC_BYTE && message.length > 5) {
            int schemaId = ByteBuffer.wrap(message, 1, 4).getInt();
            if (schemaId <= 0) {
                throw new IOException("Invalid Confluent Schema Registry ID: " + schemaId);
            }
            decoder = DecoderFactory.get().binaryDecoder(message, 5, message.length - 5, decoder);
            GenericRecord rec = datumReader.read(null, decoder);
            return new ClickEvent(
                    str(rec.get("event_id")),
                    str(rec.get("user_id")),
                    str(rec.get("client_session_id")),
                    str(rec.get("event_type")),
                    str(rec.get("product_id")),
                    ((Number) rec.get("quantity")).intValue(),
                    str(rec.get("device")),
                    str(rec.get("page_url")),
                    ((Number) rec.get("event_ts_ms")).longValue(),
                    ((Number) rec.get("created_at_ms")).longValue(),
                    Boolean.TRUE.equals(rec.get("is_synthetic_late")));
        }

        // JSON fallback for fixture testing
        JsonNode node = jsonMapper.readTree(message);
        return new ClickEvent(
                node.path("event_id").asText(),
                node.path("user_id").asText(),
                node.path("client_session_id").asText(),
                node.path("event_type").asText(),
                node.path("product_id").asText(),
                node.path("quantity").asInt(1),
                node.path("device").asText("web"),
                node.path("page_url").asText("/"),
                node.path("event_ts_ms").asLong(),
                node.path("created_at_ms").asLong(),
                node.path("is_synthetic_late").asBoolean(false));
    }

    private static String str(Object val) {
        return val == null ? "" : val.toString();
    }

    @Override
    public boolean isEndOfStream(ClickEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ClickEvent> getProducedType() {
        return TypeInformation.of(ClickEvent.class);
    }
}
