package org.folio.rulez;

/**
 * @author shale
 *
 */
public class RuleException extends Exception {

  private static final String RULE_PREFIX = "RuleException: ";

  public RuleException(String message) {
    super(RULE_PREFIX + message);
}

}
