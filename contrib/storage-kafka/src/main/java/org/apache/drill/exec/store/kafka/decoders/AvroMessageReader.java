package org.apache.drill.exec.store.kafka.decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.kafka.KafkaStoragePlugin;
import org.apache.drill.exec.store.kafka.ReadOptions;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.DrillBuf;

public class AvroMessageReader implements MessageReader {

  private static final Logger logger = LoggerFactory.getLogger(AvroMessageReader.class);

  private VectorContainerWriter writer;
  private JsonReader jsonReader;
  private Schema schema;

  @Override
  public void init(DrillBuf buf, List<SchemaPath> columns, VectorContainerWriter writer, ReadOptions readOptions) {
    this.schema = getSchema();
    this.writer = writer;
    this.jsonReader = new JsonReader.Builder(buf)
      .schemaPathColumns(columns)
      .allTextMode(readOptions.isAllTextMode())
      .readNumbersAsDouble(readOptions.isReadNumbersAsDouble())
      .enableNanInf(readOptions.isAllowNanInf())
      .enableEscapeAnyChar(readOptions.isAllowEscapeAnyChar())
      .build();

    logger.info("Initialized AvroMessageReader");
  }

  @Override
  public boolean readMessage(ConsumerRecord<?, ?> record) {
    byte[] binaryData = (byte[]) record.value();
    // TODO: Use DrillbufInputStream 
    InputStream inputStream = new ByteArrayInputStream(binaryData);
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    GenericRecord decodedData = null;
    try {
      // Marshal to java and then reuse code from JsonMessageReader
      JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, inputStream);
      decodedData = reader.read(null, jsonDecoder);
    } catch (IOException e) {
      logger.error("Failed to decode message: {}", e);
      return false;
    }

    return commitMessage(decodedData.toString());
  }

  @Override
  public KafkaConsumer<byte[], byte[]> getConsumer(KafkaStoragePlugin plugin) {
    return new KafkaConsumer<>(plugin.getConfig().getKafkaConsumerProps(),
      new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  @Override
  public void ensureAtLeastOneField() {
    jsonReader.ensureAtLeastOneField(writer);
  }

  @Override
  public void close() throws IOException {
    this.writer.clear();
    try {
      this.writer.close();
    } catch (Exception e) {
      logger.warn("Error while closing AvroMessageReader: {}", e.getMessage());
    }
  }

  private Schema getSchema() {
    return new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"person\", \"fields\": [{\"name\":\"name\", \"type\":\"string\"}, {\"name\":\"age\", \"type\":\"int\"}]}");
  }

  private boolean commitMessage(String data) {
    try {
      JsonNode jsonNode = objectMapper.readTree(data);
      if (jsonNode != null && jsonNode.isObject()) {
        ObjectNode objectNode = (ObjectNode) jsonNode;
        objectNode.put(KAFKA_TOPIC.getFieldName(), record.topic());
        objectNode.put(KAFKA_PARTITION_ID.getFieldName(), record.partition());
        objectNode.put(KAFKA_OFFSET.getFieldName(), record.offset());
        objectNode.put(KAFKA_TIMESTAMP.getFieldName(), record.timestamp());
        objectNode.put(KAFKA_MSG_KEY.getFieldName(), record.key() != null ? record.key().toString() : null);
      } else {
        throw new IOException("Unsupported node type: " + (jsonNode == null ? "NO CONTENT" : jsonNode.getNodeType()));
      }
      jsonReader.setSource(jsonNode);
      return convertJsonReadState(jsonReader.write(writer));
    } catch (IOException | IllegalArgumentException e) {
      String message = String.format("JSON record %s: %s", data, e.getMessage());
      if (jsonReader.ignoreJSONParseError()) {
        logger.debug("Skipping {}", message, e);
        return false;
      }
      throw UserException.dataReadError(e)
        .message("Failed to read " + message)
        .addContext("MessageReader", JsonMessageReader.class.getName())
        .build(logger);
    }
  }
}