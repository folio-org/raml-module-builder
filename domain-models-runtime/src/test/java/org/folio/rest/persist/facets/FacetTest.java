package org.folio.rest.persist.facets;

import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import freemarker.template.TemplateException;

public class FacetTest {

  private static final String REASON = "Generated facet field path %s does not match the expected %s";
  private static final String ALIAS_REASON = "Generated facet alias %s does not match the expected %s";
  private static final String FORMAT_REASON = "Numeric formatting does not match the expected %s";

  @Test
  public void stringsToFacet() throws IOException, TemplateException {

    List<String> facetsStrings = new ArrayList<>();

    facetsStrings.add("username:5");
    facetsStrings.add("personal.phone:5");
    facetsStrings.add("personal.phone.number:5");
    facetsStrings.add("addresses[].postalCode:5");
    facetsStrings.add("personal.addresses[].secondaryAddress.abc[].dc:5");
    facetsStrings.add("personal.secondaryAddress[].postalCode2:5");
    facetsStrings.add("addresses[].secondaryAddress[].postalCode2:5");
    facetsStrings.add("personal.secondaryAddress[].address.a[]:5");
    facetsStrings.add("personal.secondaryAddress[].address.a[].b:1000");

    List<FacetField> fList = FacetManager.convertFacetStrings2FacetFields(facetsStrings, "jsonb");

    assertThat(String.format(REASON, fList.get(0).getFieldPath(),  "jonb->>'username'"), fList.get(0).getFieldPath(),
        CoreMatchers.is( "jsonb->>'username'" ));

    assertThat(String.format(ALIAS_REASON, fList.get(0).getAlias(),  "username"), fList.get(0).getAlias(),
      CoreMatchers.is( "username" ));



    assertThat(String.format(REASON, fList.get(1).getFieldPath(),  "jsonb->'personal'->>'phone'"), fList.get(1).getFieldPath(),
      CoreMatchers.is( "jsonb->'personal'->>'phone'" ));

    assertThat(String.format(ALIAS_REASON, fList.get(1).getAlias(), "phone"), fList.get(1).getAlias(),
      CoreMatchers.is( "phone" ));


    assertThat(String.format(REASON, fList.get(2).getFieldPath(),  "jsonb->'personal'->'phone'->>'number'"), fList.get(2).getFieldPath(),
      CoreMatchers.is( "jsonb->'personal'->'phone'->>'number'" ));

    assertThat(String.format(ALIAS_REASON, fList.get(2).getAlias(),  "number"), fList.get(2).getAlias(),
      CoreMatchers.is( "number" ));


    assertThat(String.format(REASON, fList.get(3).getFieldPath(),
      "(jsonb_array_elements((jsonb->'addresses'))::jsonb)->'postalCode'"), fList.get(3).getFieldPath(),
      CoreMatchers.is( "(jsonb_array_elements((jsonb->'addresses'))::jsonb)->'postalCode'" ));

    assertThat(String.format(ALIAS_REASON, fList.get(3).getAlias(),
        "postalCode"), fList.get(3).getAlias(), CoreMatchers.is( "postalCode" ));


    assertThat(String.format(REASON, fList.get(4).getFieldPath(),
      "(jsonb_array_elements(((jsonb_array_elements((jsonb->'personal'->'addresses'))::jsonb)->'secondaryAddress'->'abc'))::jsonb)->'dc'"),
      fList.get(4).getFieldPath(),
      CoreMatchers.is(
        "(jsonb_array_elements(((jsonb_array_elements((jsonb->'personal'->'addresses'))::jsonb)->'secondaryAddress'->'abc'))::jsonb)->'dc'" ));

    assertThat(String.format(ALIAS_REASON, fList.get(4).getAlias(),"dc"),
        fList.get(4).getAlias(), CoreMatchers.is("dc"));



    assertThat(String.format(REASON, fList.get(5).getFieldPath(),
      "(jsonb_array_elements((jsonb->'personal'->'secondaryAddress'))::jsonb)->'postalCode2'"), fList.get(5).getFieldPath(),
      CoreMatchers.is( "(jsonb_array_elements((jsonb->'personal'->'secondaryAddress'))::jsonb)->'postalCode2'" ));

    assertThat(String.format(ALIAS_REASON, fList.get(5).getAlias(),
        "postalCode2"), fList.get(5).getAlias(), CoreMatchers.is( "postalCode2" ));



    assertThat(String.format(REASON, fList.get(6).getFieldPath(),
      "(jsonb_array_elements(((jsonb_array_elements((jsonb->'addresses'))::jsonb)->'secondaryAddress'))::jsonb)->'postalCode2'"),
      fList.get(6).getFieldPath(),
      CoreMatchers.is( "(jsonb_array_elements(((jsonb_array_elements((jsonb->'addresses'))::jsonb)->'secondaryAddress'))::jsonb)->'postalCode2'" ));

    assertThat(String.format(ALIAS_REASON, fList.get(6).getAlias(),
        "postalCode2"), fList.get(6).getAlias(), CoreMatchers.is( "postalCode2" ));



    assertThat(String.format(REASON, fList.get(7).getFieldPath(),
        "(jsonb_array_elements(((jsonb_array_elements((jsonb->'personal'->'secondaryAddress'))::jsonb)->'address'->'a'))::jsonb)"),
        fList.get(7).getFieldPath(),
        CoreMatchers.is( "(jsonb_array_elements(((jsonb_array_elements((jsonb->'personal'->'secondaryAddress'))::jsonb)->'address'->'a'))::jsonb)" ));

    assertThat(String.format(ALIAS_REASON, fList.get(7).getAlias(),"a"),
      fList.get(7).getAlias(), CoreMatchers.is( "a" ));



    assertThat(String.format(REASON, fList.get(8).getFieldPath(),
        "(jsonb_array_elements(((jsonb_array_elements((jsonb->'personal'->'secondaryAddress'))::jsonb)->'address'->'a'))::jsonb)->'b'"),
        fList.get(8).getFieldPath(),
        CoreMatchers.is( "(jsonb_array_elements(((jsonb_array_elements((jsonb->'personal'->'secondaryAddress'))::jsonb)->'address'->'a'))::jsonb)->'b'" ));

    assertThat(String.format(ALIAS_REASON, fList.get(8).getAlias(),"b"),
        fList.get(8).getAlias(), CoreMatchers.is( "b" ));

    FacetManager fm = new FacetManager("myuniversity_new1_mod_users.users");

    List<FacetField> facets = new ArrayList<>();
    facets.add(new FacetField("jsonb->'username[]'->'username2[]'->>'abc'", 1000));

    fm.setSupportFacets(facets);

    fm.setWhere("where username=jha* OR username=szeev*");

    fm.setMainQuery("SELECT jsonb FROM myuniversity_new1_mod_users.users where jsonb->>'username' like 'jha%' OR jsonb->>'username' like 'szeev%'" );

    String facetQuery = fm.generateFacetQuery();

    assertThat(String.format(FORMAT_REASON, "1000"), facetQuery, CoreMatchers.containsString( "jsonb FROM lst1 limit 1000" ));
    System.out.println(facetQuery);
  }
}
