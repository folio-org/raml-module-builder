package org.folio.cql2pgjson.model;

import java.util.LinkedList;
import java.util.List;

import org.folio.cql2pgjson.exception.CQLFeatureUnsupportedException;
import org.z3950.zing.cql.CQLTermNode;
import org.z3950.zing.cql.Modifier;
import org.z3950.zing.cql.ModifierSet;

public class CqlModifiers {
  private CqlSort cqlSort = CqlSort.ASCENDING;
  private CqlCase cqlCase = CqlCase.IGNORE_CASE;
  private CqlAccents cqlAccents = CqlAccents.IGNORE_ACCENTS;
  private CqlTermFormat cqlTermFormat = CqlTermFormat.STRING;
  private CqlMasking cqlMasking = CqlMasking.MASKED;
  private List<Modifier> relationModifiers = new LinkedList<>();

  public CqlModifiers(CQLTermNode node) throws CQLFeatureUnsupportedException {
    readModifiers(node.getRelation().getModifiers());
  }

  public CqlModifiers(ModifierSet modifierSet) throws CQLFeatureUnsupportedException {
    readModifiers(modifierSet.getModifiers());
  }

  /**
   * Read the modifiers and write the last for each enum into the enum variable.
   * Default is ascending, ignoreCase, ignoreAccents and masked.
   *
   * @param modifiers where to read from
   * @throws CQLFeatureUnsupportedException
   */
  @SuppressWarnings("squid:MethodCyclomaticComplexity")
  public final void readModifiers(List<Modifier> modifiers) throws CQLFeatureUnsupportedException {
    for (Modifier m : modifiers) {
      if (m.getType().startsWith("@")) {
        relationModifiers.add(m);
        continue;
      }
      switch (m.getType()) {
        case "ignorecase":
          setCqlCase(CqlCase.IGNORE_CASE);
          break;
        case "respectcase":
          setCqlCase(CqlCase.RESPECT_CASE);
          break;
        case "ignoreaccents":
          setCqlAccents(CqlAccents.IGNORE_ACCENTS);
          break;
        case "respectaccents":
          setCqlAccents(CqlAccents.RESPECT_ACCENTS);
          break;
        case "string":
          setCqlTermFormat(CqlTermFormat.STRING);
          break;
        case "number":
          setCqlTermFormat(CqlTermFormat.NUMBER);
          break;
        case "sort.ascending":
          setCqlSort(CqlSort.ASCENDING);
          break;
        case "sort.descending":
          setCqlSort(CqlSort.DESCENDING);
          break;
        case "masked":
          setCqlMasking(CqlMasking.MASKED);
          break;
        default:
          throw new CQLFeatureUnsupportedException("CQL: Unsupported modifier " + m.getType());
      }
    }
  }

  public List<Modifier> getRelationModifiers() {
    return relationModifiers;
  }

  public void setRelationModifiers(List<Modifier> relationModifiers) {
    this.relationModifiers = relationModifiers;
  }

  public CqlSort getCqlSort() {
    return cqlSort;
  }

  public void setCqlSort(CqlSort cqlSort) {
    this.cqlSort = cqlSort;
  }

  public CqlCase getCqlCase() {
    return cqlCase;
  }

  public void setCqlCase(CqlCase cqlCase) {
    this.cqlCase = cqlCase;
  }

  public CqlAccents getCqlAccents() {
    return cqlAccents;
  }

  public void setCqlAccents(CqlAccents cqlAccents) {
    this.cqlAccents = cqlAccents;
  }

  public CqlTermFormat getCqlTermFormat() {
    return cqlTermFormat;
  }

  public void setCqlTermFormat(CqlTermFormat cqlTermFormat) {
    this.cqlTermFormat = cqlTermFormat;
  }

  public CqlMasking getCqlMasking() {
    return cqlMasking;
  }

  public void setCqlMasking(CqlMasking cqlMasking) {
    this.cqlMasking = cqlMasking;
  }
}
