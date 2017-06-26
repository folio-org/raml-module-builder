package org.folio.rest.tools.client.exceptions;

/**
 * @author shale
 *
 */
public class PreviousRequestException extends Exception {


  private static final long serialVersionUID = -7772142971868252088L;
  private static final String MESSAGE_DEFAULT = "Previous Response / Response body contains errors";
  private String message;


  public PreviousRequestException() {
    super();
    this.message = MESSAGE_DEFAULT;
  }

  public PreviousRequestException(String message) {
    super(message);
    message = MESSAGE_DEFAULT + ", " + message;
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
