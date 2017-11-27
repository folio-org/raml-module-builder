-- left join between the ${view.table.tableName} and ${view.joinTable.tableName} tables
CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.${view.viewName} AS select u.${view.pkColumnName},u.jsonb as ${view.table.jsonFieldAlias}, g.jsonb as ${view.joinTable.jsonFieldAlias} from ${myuniversity}_${mymodule}.${view.table.tableName} u
 join ${myuniversity}_${mymodule}.${view.joinTable.tableName} g on 
      <#if view.table.indexUsesCaseSensitive == false>lower</#if>
        (
          <#if view.table.indexUsesRemoveAccents == true>f_unaccent(</#if>
            u.${view.table.joinOnField}
          <#if view.table.indexUsesRemoveAccents == true>)</#if>
        )  = 
        <#if view.joinTable.indexUsesCaseSensitive == false>lower</#if>
        (
          <#if view.joinTable.indexUsesRemoveAccents == true>f_unaccent(</#if>
            g.${view.joinTable.joinOnField}
          <#if view.joinTable.indexUsesRemoveAccents == true>)</#if>
        );