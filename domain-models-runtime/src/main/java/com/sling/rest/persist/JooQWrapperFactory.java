/**
 * JooQWrapperFactory
 * 
 * Jul 18, 2016
 *
 * Apache License Version 2.0
 */
package com.sling.rest.persist;

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
