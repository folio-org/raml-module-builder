package org.folio.rest.persist.interfaces;

import java.util.List;

import org.folio.rest.jaxrs.model.ResultInfo;

/**
 * @author shale
 *
 */
public class Results <T> {

  private List<T> list;
  private ResultInfo rInfo;

  public void setResults(List<T> list){
    this.list = list;
  }

  public List<T> getResults(){
    return list;
  }

  public void setResultInfo(ResultInfo info){
    this.rInfo = info;
  }

  public ResultInfo getResultInfo(){
    return this.rInfo;
  }

}
