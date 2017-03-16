package org.folio.rest.tools.utils;

import io.vertx.core.AsyncResult;

import javax.ws.rs.core.Response;

/**
 * @author shale
 *
 */
public class AsyncResponseResult implements AsyncResult<Response> {

  private Response result;
  private Throwable error;

  public AsyncResponseResult(){

  }

  @Override
  public Response result() {
    return result;
  }

  @Override
  public Throwable cause() {
    return error;
  }

  @Override
  public boolean succeeded() {
    if(error == null){
      return true;
    }
    return false;
  }

  @Override
  public boolean failed() {
    if(error != null){
      return true;
    }
    return false;
  }

  public void setResult(Response result) {
    this.result = result;
  }

  public void setError(Throwable error) {
    this.error = error;
  }



}
