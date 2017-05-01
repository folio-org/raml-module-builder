package org.folio.rest.tools.client;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.folio.rest.tools.parser.JsonPathParser;

/**
 * @author shale
 *
 */
public class BuildCQL {

  private Response r;
  private String pathToExtractFrom;
  private String cqlPath;
  private String queryParamName = "query";
  private boolean addQuestionMark = true;
  private String operatorBetweenArgs = "or";

  private BuildCQL(){}

  public BuildCQL(Response r, String pathToExtractFrom, String cqlPath, String queryParamName, boolean addQuestionMark, String operator){
    this.r = r;
    this.pathToExtractFrom = pathToExtractFrom;
    this.queryParamName = queryParamName;
    this.addQuestionMark = addQuestionMark;
    this.cqlPath = cqlPath;
    this.operatorBetweenArgs = operator;
  }

  public BuildCQL(Response r, String pathToExtractFrom, String cqlPath, String queryParamName){
    this.r = r;
    this.pathToExtractFrom = pathToExtractFrom;
    this.queryParamName = queryParamName;
    this.cqlPath = cqlPath;
  }

  public BuildCQL(Response r, String pathToExtractFrom, String cqlPath){
    this.r = r;
    this.pathToExtractFrom = pathToExtractFrom;
    this.cqlPath = cqlPath;
  }

  public String buildCQL() throws UnsupportedEncodingException {
    StringBuilder sb = new StringBuilder();
    if(addQuestionMark){
      sb.append("?");
    }
    else{
      sb.append("&");
    }
    sb.append(queryParamName).append("=");
    JsonPathParser jpp = new JsonPathParser( r.getBody() );
    Object o = jpp.getValueAt(pathToExtractFrom);
    List<Object> paths = null;
/*    if(o instanceof JsonArray){
      paths = ((JsonArray)o).getList();
    }*/
    if (o instanceof List){
      //assume list
      paths = (List<Object>)jpp.getValueAt(pathToExtractFrom);
    }
    else{
      //single value
      paths = new ArrayList<>();
      paths.add(o);
    }
    int size = paths.size();
    for (int i = 0; i < size; i++) {
      sb.append(cqlPath).append("==").append(URLEncoder.encode(paths.get(i).toString(), "UTF-8"));
      if(i<size-1){
        sb.append("+").append(operatorBetweenArgs).append("+");
      }
    }
    return sb.toString();
  }

}
