package org.apache.drill.exec.store.kafka.decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.easy.json.JsonProcessor;
import org.apache.drill.exec.store.easy.json.reader.BaseJsonProcessor;
import org.apache.drill.exec.store.kafka.KafkaStoragePlugin;
import org.apache.drill.exec.store.kafka.ReadOptions;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.drill.exec.store.kafka.MetaDataField.KAFKA_MSG_KEY;
import static org.apache.drill.exec.store.kafka.MetaDataField.KAFKA_OFFSET;
import static org.apache.drill.exec.store.kafka.MetaDataField.KAFKA_PARTITION_ID;
import static org.apache.drill.exec.store.kafka.MetaDataField.KAFKA_TIMESTAMP;
import static org.apache.drill.exec.store.kafka.MetaDataField.KAFKA_TOPIC;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.buffer.DrillBuf;

public class AvroMessageReader implements MessageReader {

  private static final Logger logger = LoggerFactory.getLogger(AvroMessageReader.class);

  private VectorContainerWriter writer;
  private JsonReader jsonReader;
  private ObjectMapper objectMapper;

  @Override
  public void init(DrillBuf buf, List<SchemaPath> columns, VectorContainerWriter writer, ReadOptions readOptions) {
    this.writer = writer;
    this.jsonReader = new JsonReader.Builder(buf)
      .schemaPathColumns(columns)
      .allTextMode(readOptions.isAllTextMode())
      .readNumbersAsDouble(readOptions.isReadNumbersAsDouble())
      .enableNanInf(readOptions.isAllowNanInf())
      .enableEscapeAnyChar(readOptions.isAllowEscapeAnyChar())
      .build();
    this.objectMapper = BaseJsonProcessor.getDefaultMapper()
      .configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, readOptions.isAllowNanInf())
      .configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, readOptions.isAllowEscapeAnyChar());

    logger.info("Initialized AvroMessageReader");
  }

  @Override
  public boolean readMessage(ConsumerRecord<?, ?> record) {
    byte[] binaryData = (byte[]) record.value();

    InputStream inputStream = new ByteArrayInputStream(binaryData);
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    
    GenericRecord decodedData = null;
    try (DataFileStream<GenericRecord> streamReader = new DataFileStream<>(inputStream, datumReader)) {
      decodedData = streamReader.next();
    } catch (IOException e) {
      logger.error("Failed to decode message: {}", e);
      return false;
    }

    return commitMessage(decodedData.toString(), record);
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

  private boolean commitMessage(String data, ConsumerRecord<?, ?> record) {
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

  private boolean convertJsonReadState(JsonProcessor.ReadState jsonReadState) {
    switch (jsonReadState) {
      case WRITE_SUCCEED:
      case END_OF_STREAM:
        return true;
      case JSON_RECORD_PARSE_ERROR:
      case JSON_RECORD_PARSE_EOF_ERROR:
        return false;
      default:
        throw new IllegalArgumentException("Unexpected JSON read state: " + jsonReadState);
    }
  }
}