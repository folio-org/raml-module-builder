package org.folio.rest.tools.client;

/**
 * @author shale
 *
 */
public class ResponseNullPointer extends Exception {

  private static final long serialVersionUID = -4654601402131347697L;
  private static final String MESSAGE_DEFAULT = "Response / Response body can not be null";
  private String message;

  public ResponseNullPointer() {
    super();
    this.message = MESSAGE_DEFAULT;
  }

  public ResponseNullPointer(String message) {
    super(message);
    this.message = message;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public String getLocalizedMessage() {
    return message;
  }

}
