/**
 * Messages
 * 
 * Aug 10, 2016
 *
 * Apache License Version 2.0
 */
package com.folio.rulez;

/**
 * @author shale
 *
 */
public class Messages {

  public static final int HELLO   = 0;
  public static final int GOODBYE = 1;
  public String          message;
  public int             status =0;
  
  public String getMessage() {
    return message;
  }
  public void setMessage(String message) {
    this.message = message;
  }
  public int getStatus() {
    return this.status;
  }
  public void setStatus(int status) {
    this.status = status;
  } 
  
}
