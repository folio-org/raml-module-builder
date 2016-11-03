package org.folio.rest.resource.interfaces;


/**
 * Interface for importing data from a file into a mongoDB
 * the interface will pass in a line from the imported file for processing.
 * Saving of data happens in bulk by the framework and information about the import
 * is saved in a table in the database.
 * CURRENTLY NOT ALL functions are consulted by the framework
 */
public interface Importer {

  public static final String LINE_SEPERATOR = System.getProperty("line.separator");

  /**
   * set the delimiter to split lines by - default is {@code System.getProperty("line.separator")}
   */
  public String getLineDelimiter();

  /**
  * fail an insert if a value in a specific fields (returned) already exists. <br>For example: we are
  * attempting to insert a record with a barcode and this barcode already exists in the table - we may want to fail the
  * insert of this specific record.
  * @return String[] - fields that should be unique values - index must be created with unique option for this to work
  */
  public String[] getFailOnExists();

  /**
   * Return Event address that will kickoff the import process. When files are uploaded the framework will start processing a specific
   * {@code Importer} implementation based on the implementation that is registered on the address passed via the upload API
   */
  public String getImportAddress();

  /**
   * @return return the collection name to save content to
   */
  public String getCollection();
  /**
   * This is the main function to implement. This function will be called with a line of data from the imported file.
   * processing should be done here on the line from the file and an object (pojo) should be returned.
   * The object will be saved by the framework.
   * @param lineFromFile
   * @return pojo to save if processing is successful or null if there are any errors and the object should not be saved
   */
  public Object processLine(String lineFromFile);

}
