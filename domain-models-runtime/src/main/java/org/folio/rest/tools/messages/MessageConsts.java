package org.folio.rest.tools.messages;

public enum MessageConsts implements MessageEnum {

  InternalServerError("10001"),
  OperationNotSupported("10002"),
  UnableToProcessRequest("10003"),
  InvalidParameters("10004"),
  HTTPMethodNotSupported("10005"),
  ContentTypeError("10006"),
  AcceptHeaderError("10007"),
  ObjectDoesNotExist("10008");
  
  private String code;
  
  private MessageConsts(String code){
    this.code = code;
  }
  
  public String getCode(){
    return code;
  }
  
}
