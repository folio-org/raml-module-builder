package org.folio.rest.tools.messages;

public enum MessageConsts implements MessageEnum {
  
  InitializeVerticleFail("10000"),
  InternalServerError("10001"),
  OperationNotSupported("10002"),
  UnableToProcessRequest("10003"),
  InvalidParameters("10004"),
  HTTPMethodNotSupported("10005"),
  ContentTypeError("10006"),
  AcceptHeaderError("10007"),
  ObjectDoesNotExist("10008"),
  ImportFailed("10009"),
  InvalidURLPath("10010"),
  FileUploadError("10011"); 
  
  private String code;
  
  private MessageConsts(String code){
    this.code = code;
  }
  
  public String getCode(){
    return code;
  }
  
}
