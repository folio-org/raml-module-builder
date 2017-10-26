package org.folio.rest.tools.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Set;

/**
 * Copy a RAML directory tree to a target directory tree and
 * dereference all $ref references of all .json and .schema files.
 */
public class RamlDirCopier extends SimpleFileVisitor<Path> {
  private final Path baseSourceDir;
  private final Path baseTargetDir;
  private final SchemaDereferencer schemaDereferencer;

  RamlDirCopier(Path sourceDir, Path targetDir) {
    this.baseSourceDir = sourceDir;
    this.baseTargetDir = targetDir;
    schemaDereferencer = new SchemaDereferencer();
  }

  @Override
  public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
      throws IOException {

    // create destination directory with same name
    Path targetDir = baseTargetDir.resolve(baseSourceDir.relativize(dir));
    try {
System.err.println("copy dir: " + dir + " -> " + targetDir);
      Files.copy(dir, targetDir);
    } catch (FileAlreadyExistsException x) {
      // ignore
    }
    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
    Path sourcePath = baseSourceDir.relativize(file);
    String sourceString = sourcePath.toString();
    Path targetFile = baseTargetDir.resolve(sourcePath);
    if (sourceString.matches(".*(\\.json|\\.schema)$")) {
System.err.println("dereference: " + sourceString + " -> " + targetFile);
      String json = schemaDereferencer.dereferencedSchema(file).encodePrettily();
      try (PrintWriter printWriter = new PrintWriter(targetFile.toFile())) {
        printWriter.println(json);
      }
    } else {
System.err.println("copy file: " + file + " -> " + targetFile);
      Files.copy(file, targetFile);
    }
    return FileVisitResult.CONTINUE;
  }

  /**
   * Copy a RAML directory tree to a target directory tree and
   * dereference all $ref references of all .json and .schema files.
   *
   * @param sourceDir  directory tree to copy
   * @param targetDir  target directory
   * @throws IOException  on read or write error
   */
  public static void copy(Path sourceDir, Path targetDir) throws IOException {
    File targetFile = targetDir.toFile();
    if (! targetFile.isDirectory()) {
      targetFile.mkdirs();
    }
    Set<FileVisitOption> options = Collections.emptySet();
    RamlDirCopier ramlDirCopier = new RamlDirCopier(sourceDir, targetDir);
    Files.walkFileTree(sourceDir, options, Integer.MAX_VALUE, ramlDirCopier);
  }
}
