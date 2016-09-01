package com.sling.rest.persist;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultExecuteListenerProvider;

import com.sling.rest.persist.listeners.StatisticsListener;

/**
 * @author shale
 *
 */
public class JooQWrapper {

  private DSLContext create = null;
  
  protected JooQWrapper(String schema) throws Exception {
    
    Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "mysecretpassword");
    connection.setSchema(schema);    
    // Create a configuration with an appropriate listener provider:
    Configuration configuration = new DefaultConfiguration().set(connection).set(SQLDialect.POSTGRES_9_5);
    configuration.set(new DefaultExecuteListenerProvider(new StatisticsListener()));
    create = DSL.using(configuration);
    //Table<Record> table = DSL.table("test.item");
  }
  
  private Map<Field<?>, Object> mapProperties(Object bean) throws Exception {
    Map<Field<?>, Object> properties = new HashMap<>();
    for (PropertyDescriptor property : Introspector.getBeanInfo(bean.getClass()).getPropertyDescriptors()) {
        String name = property.getName();
        Class<?> type = property.getPropertyType();        
        Object value = property.getReadMethod().invoke(bean);        
        Field<?> field = DSL.field(name, type);
        properties.put(field, value);
    }
    return properties;
  }
  
  public void insert(String tableName, Object bean) throws Exception {
    //Table<Record> table = DSL.table("test.item");
    Table<Record> table = DSL.table(tableName);
    create.insertInto(table).set(mapProperties(bean));
  }
  
  public void update(String tableName, Object bean) throws Exception {
    Table<Record> table = DSL.table(tableName);
    create.update(table).set(mapProperties(bean));
  }
  
  public void delete(String tableName, Condition... conditions) throws Exception {
    Table<Record> table = DSL.table(tableName);
    create.delete(table).where(conditions);
  }
  
  public void retrieve(String tableName, Condition... conditions) throws Exception {
    Table<Record> table = DSL.table(tableName);
    create.delete(table).where(conditions);
  }
  
  /**
   * 
   * @param sql - sql = "(X = 1 and Y = 2)";
   * @return
   */
  public Condition createCondition(String sql){
    return DSL.condition(sql);
  }
  
  public Condition createCondition(String field, Object value){
    Field<?> f = DSL.field(field);
    HashMap<Field<?>, Object> hm = new HashMap<>();
    hm.put(f, value);
    return DSL.condition(hm);
  }
  
  public Table createTable(String table){
    Table<?> t = DSL.table(table);
    return t;
  }
  
  public DSLContext getDSLContext(){
    return create;
  }
  
}
