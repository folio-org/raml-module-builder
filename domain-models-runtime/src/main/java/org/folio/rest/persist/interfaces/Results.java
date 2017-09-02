package org.folio.rest.persist.interfaces;

import java.util.List;

import org.folio.rest.jaxrs.model.ResultInfo;

/**
 * @author shale
 *
 */
public class Results {

  private List<? extends Object> list;
  private ResultInfo rInfo;

  public void setResults(List<?> list){
    this.list = list;
  }

  public List<?> getResults(){
    return list;
  }

  public void setResultInfo(ResultInfo info){
    this.rInfo = info;
  }

  public ResultInfo getResultInfo(){
    return this.rInfo;
  }

}
