import type { SQLConfig } from '@codemirror/lang-sql';

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
