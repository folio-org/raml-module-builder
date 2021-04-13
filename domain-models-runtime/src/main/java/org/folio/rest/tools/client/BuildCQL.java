package org.folio.rest.tools.client;

import java.util.ArrayList;
import java.util.List;

import org.folio.rest.tools.parser.JsonPathParser;
import org.folio.util.StringUtil;

/**
 * Build CQL string by parsing response JSON
 * @deprecated All material in org.folio.rest.tools.client is deprecated
 */
@Deprecated
public class BuildCQL {

  private Response r;
  private String pathToExtractFrom;
  private String cqlPath;
  private String queryParamName = "query";
  private boolean addQuestionMark = true;
  private String operatorBetweenArgs = "or";
  private String cqlStatementOperator = "==";


  /**
   * Builds a simple cql string by parsing through the Response's json, extracting values found in
   * the pathToExtractFrom path ({@link org.folio.rest.tools.parser.JsonPathParser}) and creating
   * a cql query string with cqlPath=values_found_in_path.
   * <br>
   * This could look something like this:
   * values found in <i>pathToExtractFrom</i>: <b>"hi", "hello", "whats up"</b>
   * <br>
   * <i>cqlPath</i>: <b>myField</b>
   * <br>
   * <i>result</i>: <b>?query=myField=hi+or+myField=hello+or+myField=whats%20up</b>
   * <br>The operator is 'or' by default
   * @param r
   * @param pathToExtractFrom
   * @param cqlPath
   * @param queryParamName - defaults to 'query'
   * @param addQuestionMark - defaults to 'true', if other params have already been added to the endpoint
   * change to false
   * @param operator - defaults to 'or'
   * @param cqlStatementOperator - operator within cql statement - default is == , for example -
   * field_a==value, can pass in for example = , to generate statements like field_a=value
   */
  public BuildCQL(Response r, String pathToExtractFrom, String cqlPath, String queryParamName, boolean addQuestionMark,
      String operator, String cqlStatementOperator){
    this.r = r;
    this.pathToExtractFrom = pathToExtractFrom;
    this.queryParamName = queryParamName;
    this.addQuestionMark = addQuestionMark;
    this.cqlPath = cqlPath;
    this.operatorBetweenArgs = operator;
    this.cqlStatementOperator = cqlStatementOperator;
  }

  /**
   * Builds a simple cql string by parsing through the Response's json, extracting values found in
   * the pathToExtractFrom path ({@link org.folio.rest.tools.parser.JsonPathParser}) and creating
   * a cql query string with cqlPath=values_found_in_path.
   * <br>
   * This could look something like this:
   * values found in <i>pathToExtractFrom</i>: <b>"hi", "hello", "whats up"</b>
   * <br>
   * <i>cqlPath</i>: <b>myField</b>
   * <br>
   * <i>result</i>: <b>?query=myField=hi+or+myField=hello+or+myField=whats%20up</b>
   * <br>The operator is 'or' by default
   * @param r
   * @param pathToExtractFrom
   * @param cqlPath
   * @param queryParamName - defaults to 'query'
   * @param addQuestionMark - defaults to 'true', if other params have already been added to the endpoint
   * change to false
   * @param operator - defaults to 'or'
   */
  public BuildCQL(Response r, String pathToExtractFrom, String cqlPath, String queryParamName, boolean addQuestionMark, String operator){
    this.r = r;
    this.pathToExtractFrom = pathToExtractFrom;
    this.queryParamName = queryParamName;
    this.addQuestionMark = addQuestionMark;
    this.cqlPath = cqlPath;
    this.operatorBetweenArgs = operator;
  }

  /**
   * Builds a simple cql string by parsing through the Response's json, extracting values found in
   * the pathToExtractFrom path ({@link org.folio.rest.tools.parser.JsonPathParser}) and creating
   * a cql query string with cqlPath=values_found_in_path.
   * <br>
   * This could look something like this:
   * values found in <i>pathToExtractFrom</i>: <b>"hi", "hello", "whats up"</b>
   * <br>
   * <i>cqlPath</i>: <b>myField</b>
   * <br>
   * <i>result</i>: <b>?query=myField=hi+or+myField=hello+or+myField=whats%20up</b>
   * <br>The operator is 'or' by default
   * @param r
   * @param pathToExtractFrom
   * @param cqlPath
   * @param queryParamName - defaults to 'query'
   */
  public BuildCQL(Response r, String pathToExtractFrom, String cqlPath, String queryParamName){
    this.r = r;
    this.pathToExtractFrom = pathToExtractFrom;
    this.queryParamName = queryParamName;
    this.cqlPath = cqlPath;
  }

  /**
   * Builds a simple cql string by parsing through the Response's json, extracting values found in
   * the pathToExtractFrom path ({@link org.folio.rest.tools.parser.JsonPathParser}) and creating
   * a cql query string with cqlPath=values_found_in_path.
   * <br>
   * This could look something like this:
   * values found in <i>pathToExtractFrom</i>: <b>"hi", "hello", "whats up"</b>
   * <br>
   * <i>cqlPath</i>: <b>myField</b>
   * <br>
   * <i>result</i>: <b>?query=myField=hi+or+myField=hello+or+myField=whats%20up</b>
   * <br>The operator is 'or' by default
   * @param r
   * @param pathToExtractFrom
   * @param cqlPath
   */
  public BuildCQL(Response r, String pathToExtractFrom, String cqlPath){
    this.r = r;
    this.pathToExtractFrom = pathToExtractFrom;
    this.cqlPath = cqlPath;
  }

  /**
   * this function is needed in cases where chaining is used so that the
   * response is injected into a Function when working with completeableFutures
   * @param r
   */
  public void setResponse(Response r){
    this.r = r;
  }

  @SuppressWarnings("unchecked")
  public String buildCQL() {
    StringBuilder sb = new StringBuilder();
    StringBuilder prefix = new StringBuilder();

    if(addQuestionMark){
      prefix.append("?");
    }
    else{
      prefix.append("&");
    }
    prefix.append(queryParamName).append("=");

    JsonPathParser jpp = new JsonPathParser( r.getBody() );
    Object o = jpp.getValueAt(pathToExtractFrom);
    List<Object> paths = null;
    /*if(o instanceof JsonArray){
      paths = ((JsonArray)o).getList();
    }*/
    if (o instanceof List){
      //assume list
      paths = (List<Object>)o;
    }
    else{
      //single value
      paths = new ArrayList<>();
      paths.add(o);
    }
    int size = paths.size();
    for (int i = 0; i < size; i++) {
      if(paths.get(i) == null){
        continue;
      }
      sb.append(cqlPath).append(cqlStatementOperator).append(paths.get(i).toString());
      if(i<size-1){
        sb.append(" ").append(operatorBetweenArgs).append(" ");
      }
    }
    if(sb.length() > 0){
      return prefix.append(StringUtil.urlEncode(sb.toString())).toString();
    }
    return "";
  }

}
