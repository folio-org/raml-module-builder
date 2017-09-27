package org.folio.rest.tools.codecs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


/**
 * serialize pojos to pass on the event bus -
 * has not been tested on a cluster - only on local messages
 *
 */
public class PojoEventBusCodec implements MessageCodec<Object, Object> {

  private static final Logger log = LoggerFactory.getLogger(PojoEventBusCodec.class);
  private static final ObjectMapper MAPPER  = new ObjectMapper();


  @Override
  public void encodeToWire(Buffer buffer, Object pojo) {

    String value = null;
    try {
      value = MAPPER.writeValueAsString(pojo);
    } catch (JsonProcessingException e) {
      log.error(e.getMessage(), e);
    }

    int dataLength = value.getBytes().length;

    String clazz = pojo.getClass().getName();

    int clazzLength = clazz.getBytes().length;

    // Write data into given buffer
    buffer.appendInt(clazzLength);
    buffer.appendString(clazz);
    buffer.appendInt(dataLength);
    buffer.appendString(value);
  }

  @Override
  public Object decodeFromWire(int position, Buffer buffer) {

    int _pos = position;

    // Length
    int clazzNameLength = buffer.getInt(_pos);
    String clazz = buffer.getString(_pos+=4, _pos+=clazzNameLength);

    // Jump 4 because getInt() == 4 bytes
    int dataLength = buffer.getInt(_pos+=4);

    String data = buffer.getString(_pos+=4, _pos+=dataLength);

    Object obj = null;
    try {
      obj = MAPPER.readValue(data, Class.forName(clazz));
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }

    // We can finally create custom message object
    return obj;
  }

  @Override
  public Object transform(Object object) {
    // If a message is sent *locally* across the event bus.
    // This example sends message just as is
    return object;
  }

  @Override
  public String name() {
    // Each codec must have a unique name.
    // This is used to identify a codec when sending a message and for unregistering codecs.
    return this.getClass().getSimpleName();
  }

  @Override
  public byte systemCodecID() {
    // Always -1
    return -1;
  }
}
