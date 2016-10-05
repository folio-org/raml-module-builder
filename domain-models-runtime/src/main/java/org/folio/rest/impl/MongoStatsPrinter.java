package org.folio.rest.impl;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;

import org.folio.rest.persist.MongoCRUD;
import org.folio.rest.resource.interfaces.PeriodicAPI;

public class MongoStatsPrinter implements PeriodicAPI {

  private static final io.vertx.core.logging.Logger log               = LoggerFactory.getLogger(MongoStatsPrinter.class);

  private static HashMap<String, Integer> collectionWhenToRunMap = new HashMap<>();
  private static Hashtable<String, Integer> collectionLastRunMap = new Hashtable<>();

  private static Integer delay = 10000; //milliseconds
  
  @Override
  public long runEvery() {
    return delay;
  }

  
  @Override
  public void run(Vertx vertx, Context context) {
    
    //create a copy of the map with the collections and the interval to run them in
    HashMap<String, Integer> tempMap = new HashMap<>();
    tempMap.putAll(collectionWhenToRunMap);
    Iterator<Map.Entry<String, Integer>> iter = tempMap.entrySet().iterator();
    
    //iterate over the collections and print stats to log for collection when interval is up
    //for the collection - run will be called every 'delay' milli to execute this
    while (iter.hasNext()) {
      Map.Entry<String,Integer> entry = iter.next();
      String collection = entry.getKey();
      Integer countdown = collectionLastRunMap.get(collection)-(delay/1000);
      if(countdown <= 0){
        printStatsFor(collection, vertx);
        countdown = collectionWhenToRunMap.get(collection);
        collectionLastRunMap.put(collection, countdown);
      }
      else{
        collectionLastRunMap.put(collection, countdown);
      }
    }
  }
  
  /**
   * @param collection
   */
  private void printStatsFor(String collection, Vertx vertx) {
    MongoCRUD.getInstance(vertx).getStatsForCollection(collection, reply -> {
      if(reply.succeeded()){
        log.info(reply.result().encode());
      }
      else{
        log.error("unable to print stats for collection " + collection + ", error: " + reply.cause().getMessage());
        //remove collection from map - require user to re-insert it if they want stats - avoids garbage in the map
        collectionLastRunMap.remove(collection);
        collectionWhenToRunMap.remove(collection);
      }
    });    
  }


  public static void addCollection(JsonObject job){
    job.forEach( i -> {
      String collection = i.getKey();
      Integer interval = (Integer)i.getValue();
      collectionWhenToRunMap.put(collection, interval);
      collectionLastRunMap.put(collection, interval);
    });
  }

  public static JsonObject getCollection(){
    JsonObject job = new JsonObject();
    BiConsumer<String, Integer> biConsumer = (key,value) -> job.put(key, value);
    collectionWhenToRunMap.forEach(biConsumer);
    return job;
  }
}
