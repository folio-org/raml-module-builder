package org.folio.rest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.text.MatchesPattern.matchesPattern;
import static org.hamcrest.MatcherAssert.assertThat;

import org.folio.rest.tools.utils.ModuleName;
import org.junit.jupiter.api.Test;

class ModuleNameTest {

  @Test
  void moduleName() {
    assertThat(ModuleName.getModuleName(), is("raml_module_builder"));
    assertThat(ModuleName.getModuleVersion(), matchesPattern("\\d+\\.\\d+\\.\\d.*"));
  }

}
