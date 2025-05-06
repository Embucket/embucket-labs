import type { NavigationTreeDatabase } from '@/orval/models';

interface Option {
  value: string;
  label: string;
}

export const transformNavigationTreesToSelectOptions = (
  navigationTrees: NavigationTreeDatabase[],
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
    db.schemas.forEach((schema) => {
      schemas.push({
        value: schema.name,
        label: schema.name,
      });
      schema.tables.forEach((table) => {
        tables.push({
          value: table.name,
          label: table.name,
        });
      });
    });
  });

  return {
    databasesOptions: databases,
    schemasOptions: schemas,
    tablesOptions: tables,
  };
};
