package org.folio.rest.impl;

import org.folio.rest.resource.interfaces.Importer;

/**
 * This is a demo importer implementation - used to just print out to console the contents
 * of an uploaded file - while registering to get indications of the uploaded file on address a.b.c
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
