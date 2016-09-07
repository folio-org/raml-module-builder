package com.folio.rest.persist;

import java.util.HashMap;

/**
 * @author shale
 *
 */
public enum JooQWrapperFactory {

  INSTANCE;

  private static HashMap<String, JooQWrapper> cache = new HashMap<>();
  
  public JooQWrapper getInstance(String schema) throws Exception {
    if(cache.get(schema) == null){
      synchronized(JooQWrapperFactory.class){
          if(cache.get(schema) == null) {
            cache.put(schema, new JooQWrapper(schema));
          }
      }
    }
    return cache.get(schema);
  }
  
  public static void reset(){
    cache.clear();
  }
  
}
