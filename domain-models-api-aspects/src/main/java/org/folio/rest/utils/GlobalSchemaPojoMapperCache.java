package org.folio.rest.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author shale
 *
 */
public class GlobalSchemaPojoMapperCache {

  private static Map<Object, Object> schema2PojoMapper = new HashMap<>();

  public static Map<Object, Object> getSchema2PojoMapper() {
    return schema2PojoMapper;
  }

  public static void add(Object key, Object value){
    schema2PojoMapper.put(key, value);
  }
}
