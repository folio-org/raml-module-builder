package org.folio.rest.persist.Criteria;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shale
 *
 */
public class GroupedCriterias {

  String groupOp = " AND ";
  List<Pairs> criterias = new ArrayList<>();

  public GroupedCriterias(){}

  public GroupedCriterias addCriteria(Criteria c){
    criterias.add(new Pairs(c, "AND"));
    return this;
  }

  public GroupedCriterias addCriteria(Criteria c, String withOp){
    criterias.add(new Pairs(c, withOp));
    return this;
  }

  public void setGroupOp(String op){
    groupOp = op;
  }

  class Pairs {

    Criteria criteria;
    String op;

    public Pairs(Criteria c, String op) {
      super();
      this.criteria = c;
      this.op = op;
    }

  }
}
