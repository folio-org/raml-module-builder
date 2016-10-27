package org.folio.rest.impl;

import org.folio.rest.resource.interfaces.Importer;

/**
 *
 */
public class ImporterDemo implements Importer {

  /* (non-Javadoc)
   * @see org.folio.rest.resource.interfaces.Importer#getLineDelimiter()
   */
  @Override
  public String getLineDelimiter() {
    return Importer.LINE_SEPERATOR;
  }

  /* (non-Javadoc)
   * @see org.folio.rest.resource.interfaces.Importer#getFailOnExists()
   */
  @Override
  public String[] getFailOnExists() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.folio.rest.resource.interfaces.Importer#getBulkSize()
   */
  @Override
  public int getBulkSize() {
    // TODO Auto-generated method stub
    return 1;
  }

  /* (non-Javadoc)
   * @see org.folio.rest.resource.interfaces.Importer#getFailPercent()
   */
  @Override
  public double getFailPercent() {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see org.folio.rest.resource.interfaces.Importer#getImportAddress()
   */
  @Override
  public String getImportAddress() {
    // TODO Auto-generated method stub
    return "a.b.c";
  }

  /* (non-Javadoc)
   * @see org.folio.rest.resource.interfaces.Importer#getCollection()
   */
  @Override
  public String getCollection() {
    // TODO Auto-generated method stub
    return "test";
  }

  /* (non-Javadoc)
   * @see org.folio.rest.resource.interfaces.Importer#processLine(java.lang.String)
   */
  @Override
  public Object processLine(String lineFromFile) {
    // TODO Auto-generated method stub
    System.out.print("in importer demo "+lineFromFile);
    return null;
  }

}
