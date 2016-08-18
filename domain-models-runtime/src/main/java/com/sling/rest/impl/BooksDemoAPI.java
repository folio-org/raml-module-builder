/**
 * BooksDemoAPI
 * 
 * Aug 18, 2016
 *
 * Apache License Version 2.0
 */
package com.sling.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;

import java.math.BigDecimal;

import javax.ws.rs.core.Response;

import com.sling.rest.annotations.Validate;
import com.sling.rest.jaxrs.model.Book;
import com.sling.rest.jaxrs.resource.BooksResource;

/**
 * @author shale
 *
 */
public class BooksDemoAPI implements BooksResource {

  
  /**
   * validate to test the validation aspect
   */
  @Validate
  @Override
  public void getBooks(String arg0, BigDecimal arg1, BigDecimal arg2, String arg3, Handler<AsyncResult<Response>> arg4, Context arg5)
      throws Exception {
    arg4.handle(io.vertx.core.Future.succeededFuture(GetBooksResponse.withJsonOK(new Book())));

  }

  /**
   * not implemented
   */
  @Override
  public void putBooks(String arg0, Handler<AsyncResult<Response>> arg1, Context arg2) throws Exception {
    arg1.handle(io.vertx.core.Future.succeededFuture(null));

  }

}
