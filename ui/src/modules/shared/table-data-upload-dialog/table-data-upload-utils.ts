import type { NavigationTreeDatabase } from '@/orval/models';

interface Option {
  value: string;
  label: string;
}

export const transformNavigationTreesToSelectOptions = (
  navigationTrees: NavigationTreeDatabase[],
  selectedDatabase?: string,
  selectedSchema?: string,
): {
  databasesOptions: Option[];
  schemasOptions: Option[];
  tablesOptions: Option[];
} => {
  const databases: Option[] = [];
  const schemas: Option[] = [];
  const tables: Option[] = [];

  navigationTrees.forEach((db) => {
    databases.push({
      value: db.name,
      label: db.name,
    });

    if (!selectedDatabase || db.name === selectedDatabase) {
      db.schemas.forEach((schema) => {
        schemas.push({
          value: schema.name,
          label: schema.name,
        });

        if (!selectedSchema || schema.name === selectedSchema) {
          schema.tables.forEach((table) => {
            tables.push({
              value: table.name,
              label: table.name,
            });
          });
        }
      });
    }
  });

  return {
    databasesOptions: databases,
    schemasOptions: schemas,
    tablesOptions: tables,
  };
};
