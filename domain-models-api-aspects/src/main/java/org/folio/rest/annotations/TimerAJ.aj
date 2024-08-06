package org.folio.rest.annotations;

import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.aspectj.lang.reflect.MethodSignature;

import io.vertx.core.Handler;

/**
 * timer aspect for async calls - create two joint points 1. on functions that use the @Timer annotation 2. when the async failed() or
 * succeeded() is called since the failed() function can be called only when the handler is called back - this gives a good indicator of
 * performance from the time the function is called (joint point #1) until the call back is called (joint point #2) this doesnt take into
 * account processing within the callback - but hopefully that should be minor
 */
public aspect TimerAJ { //pertarget(timerCall1()) { // percflow(timerCall2()){
/*  private long            timer  = 0;
  private String          methodName;
  private UUID            id ;
  private static Logger logger = LogManager.getLogger(TimerAJ.class);

  // aspectj maven plugin <includes> controls compile scope - see projects depending on this one for
  // examples

  pointcut timerCall2() : target(io.vertx.core.AsyncResult); // call(* io.vertx.ext.sql.SQLConnection.*(..));
    //target(io.vertx.core.Handler) ; // target(io.vertx.core.AsyncResult) ;
    //   && (call(* io.vertx.core.AsyncResult.failed(..)) || call(* io.vertx.core.AsyncResult.succeeded(..)));

  // indicate which classes to compile against so that it doesnt get called on every handle() call
  pointcut timerCall1() :  execution(@Timer * *(..));// || execution(* io.vertx.core.AsyncResult.*(..));// execution(@Timer * *(..));

  void around() : timerCall1() {
    //SomeObject foo = makeSomeObject();
    System.out.println("id = " + id + " method " +methodName+ " in timer ------------------->" + timer + " ------------------- " + System.currentTimeMillis() + " location " +thisEnclosingJoinPointStaticPart.getSourceLocation().getLine() );
    proceed();
    System.out.println(" in timer ------------------->" + timer + " ------------------- " + System.currentTimeMillis() + " location " +thisEnclosingJoinPointStaticPart.getSourceLocation().getLine() );

    //foo.magic();
  }

  before() : timerCall2() {
    id = UUID.randomUUID();
    methodName = thisJoinPoint.getSignature().getName();
    System.out.println("id = " + id + " method " +methodName+ " in timer ------------------->" + timer + " ------------------- " + System.currentTimeMillis() + " location " +thisEnclosingJoinPointStaticPart.getSourceLocation().getLine() );
    timer = System.currentTimeMillis();

    //logger.info("in timer ------------------->");
  }

  after() : timerCall2()  { // cflow( timerCall1() && timerCall2()) {
    System.out.println("id = " + id + " method " + methodName + " in timer ------------------->" + timer + " location " + thisEnclosingJoinPointStaticPart.getSourceLocation().getLine());
    logger.info("{} {} took {} ms",
      thisEnclosingJoinPointStaticPart.getSourceLocation().getWithinType().getName(),
      methodName,
      (System.currentTimeMillis() - timer));
  }*/
}
