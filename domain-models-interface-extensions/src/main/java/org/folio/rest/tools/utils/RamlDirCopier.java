package org.folio.rest.tools.utils;

import java.io.File;
import java.io.IOException;
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

  private SimpleFileVisitor<Path> fileVisitor = new SimpleFileVisitor<Path>() {
    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        throws IOException {

      // create destination directory with same name
      Path targetDir = baseTargetDir.resolve(baseSourceDir.relativize(dir));
      try {
        Files.copy(dir, targetDir);
      } catch (FileAlreadyExistsException x) {
        // ignore
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      Path sourcePath = baseSourceDir.relativize(file);
      Path targetPath = baseTargetDir.resolve(sourcePath);
/*      if (sourcePath.toString().matches(".*(\\.json|\\.schema)$")) {
        String json = schemaDereferencer.dereferencedSchema(file).encodePrettily();
        try (PrintWriter printWriter = new PrintWriter(targetPath.toFile())) {
          printWriter.println(json);
        }
      } else {
        Files.copy(file, targetPath, StandardCopyOption.REPLACE_EXISTING);
      }*/
      return FileVisitResult.CONTINUE;
    }
  };

  RamlDirCopier(Path sourceDir, Path targetDir) {
    this.baseSourceDir = sourceDir;
    this.baseTargetDir = targetDir;
    schemaDereferencer = new SchemaDereferencer();
  }

  /**
   * Copy a RAML directory tree to a target directory tree and
   * dereference all $ref references of all .json and .schema files.
   * It does not delete any existing files or directories at target,
   * but a file may get overwritten.
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
    Files.walkFileTree(sourceDir, options, Integer.MAX_VALUE, ramlDirCopier.fileVisitor);
  }
}
