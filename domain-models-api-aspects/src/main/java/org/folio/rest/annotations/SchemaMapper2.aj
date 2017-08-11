package org.folio.rest.annotations;

import org.aspectj.lang.annotation.SuppressAjWarnings;
import org.aspectj.lang.reflect.MethodSignature;
 

public aspect SchemaMapper2 {
  
  pointcut mapSchema2Pojo(): execution(* org.jsonschema2pojo.SchemaMapper.generate(..));
  
  @SuppressAjWarnings({"adviceDidNotMatch"})
  after() returning(Object r): mapSchema2Pojo() {
    MethodSignature methodSignature = (MethodSignature) thisJoinPoint.getSignature();
    Object params[] = new Object[2];
    if(thisJoinPoint.getArgs() != null){
      Object[] signatureArgs = thisJoinPoint.getArgs();
      //this is the schema path
      params[0] = ((java.net.URL)signatureArgs[3]);
    }
    if(r != null){
      //this is the pojo generated for that schema
      params[1] = ((com.sun.codemodel.JType)r).fullName();
    }
    write(params);
  }
  
  private void write(Object[] params){
    if(params[0] != null && params[1] != null){
      org.folio.rest.utils.GlobalSchemaPojoMapperCache.add(params[0], params[1]);
    }
  }
}
