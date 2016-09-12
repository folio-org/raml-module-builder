package org.folio.rest.tools;

public enum MessageConsts {

  InternalServerError("10001"),
  OperationNotSupported("10002"),
  UnableToProcessRequest("10003"),
  InvalidParameters("10004"),
  HTTPMethodNotSupported("10005"),
  ContentTypeError("10006"),
  AcceptHeaderError("1007"),
  ObjectDoesNotExist("1008");
  
  private String code;
  
  private MessageConsts(String code){
    this.code = code;
  }
  
  protected String getCode(){
    return code;
  }
  
}
