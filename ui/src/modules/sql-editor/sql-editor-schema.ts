/* eslint-disable @typescript-eslint/no-unsafe-member-access */
import type { SQLConfig, SQLNamespace } from '@codemirror/lang-sql';

import type { NavigationTreeDatabase } from '@/orval/models';

export const SQL_EDITOR_SCHEMA_SQL_CONFIG: SQLConfig = {
  upperCaseKeywords: true,
  schema: {
    // Databases
    database1: {
      self: { label: 'database1', type: 'database' },
      // Schemas
      children: {
        schema1: {
          self: { label: 'schema1', type: 'schema' },
          // Tables
          children: {
            table1: {
              self: { label: 'table1', type: 'table' },
              // Fields
              children: [],
              // children: [],
            },
          },
        },
      },
    },
  },
  tables: [
    { label: 'field1', type: 'int' },
    { label: 'field2', type: 'int' },
  ],
};

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
      const schemaTables = schema.tables;

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
