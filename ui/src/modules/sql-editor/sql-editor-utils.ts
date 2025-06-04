/* eslint-disable @typescript-eslint/no-unsafe-member-access */
import type { SQLNamespace } from '@codemirror/lang-sql';

import type { NavigationTreeDatabase, NavigationTreeTable } from '@/orval/models';

export const transformNavigationTreeToSqlConfigSchema = (
  navigationTree?: NavigationTreeDatabase[],
): SQLNamespace => {
  const sqlConfig: SQLNamespace = {};

  if (!navigationTree) {
    return sqlConfig;
  }

  navigationTree.forEach((db) => {
    const dbName = db.name;
    const dbSchemas = db.schemas;

    sqlConfig[dbName] = {
      self: { label: dbName, type: 'database' },
      children: {},
    };

    dbSchemas.forEach((schema) => {
      const schemaName = schema.name;
      const schemaTables: NavigationTreeTable[] = [...schema.tables, ...schema.views];

      const schemaSelf = { label: schemaName, type: 'schema' };
      const schemaChildren = {} as SQLNamespace;

      // @ts-expect-error Ignore TypeScript error for children
      sqlConfig[dbName].children[schemaName] = {
        self: schemaSelf,
        children: schemaChildren,
      };

      schemaTables.forEach((table) => {
        const tableName = table.name;

        // @ts-expect-error Ignore TypeScript error for children
        sqlConfig[dbName].children[schemaName].children[tableName] = {
          self: { label: tableName, type: 'table' },
          children: [],
        };
      });
    });
  });

  return sqlConfig;
};
