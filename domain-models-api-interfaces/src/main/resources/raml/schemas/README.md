As a workaround to FOLIO-573, when any schema file refers to an additional schema file,
then the filename of the second referenced schema must be exactly the same as the
"key" name in the RAML "schemas" section, which is being used in the $ref of the parent schema.
For each such schema, please git add and commit a symbolic link to facilitate this.
