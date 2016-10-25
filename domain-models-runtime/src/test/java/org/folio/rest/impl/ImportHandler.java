package org.folio.rest.impl;

import org.folio.rest.resource.interfaces.Importer;

public class ImportHandler implements Importer {

  @Override
  public String getLineDelimiter() {
    return Importer.LINE_SEPERATOR;
  }


  @Override
  public String[] getFailOnExists() {
    return null;
  }

  @Override
  public int getBulkSize() {
    return 1;
  }

  @Override
  public double getFailPercent() {
    return 100.00;
  }

  @Override
  public String getImportAddress() {
    return "uploads.import.generic";
  }

  @Override
  public Object processLine(String lineFromFile) {
    return lineFromFile;
  }

  @Override
  public String getCollection() {
    return "myCollection";
  }

}
