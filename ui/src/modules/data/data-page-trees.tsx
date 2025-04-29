import {
  TreesDatabases,
  TreesLayout,
  TreesSchemas,
  TreesTables,
} from '@/modules/shared/trees/trees-items';
import type {
  NavigationTreeDatabase,
  NavigationTreeSchema,
  NavigationTreeTable,
} from '@/orval/models';
import { useGetNavigationTrees } from '@/orval/navigation-trees';

export function DataPageTrees() {
  const { data: { items: navigationTrees } = {}, isFetching: isFetchingNavigationTrees } =
    useGetNavigationTrees();

  const handleDatabaseClick = (database: NavigationTreeDatabase) => {
    // eslint-disable-next-line no-console
    console.log(database);
  };

  const handleSchemaClick = (schema: NavigationTreeSchema) => {
    // eslint-disable-next-line no-console
    console.log(schema);
  };

  const handleTableClick = (table: NavigationTreeTable) => {
    // eslint-disable-next-line no-console
    console.log(table);
  };

  return (
    <>
      <TreesLayout>
        <TreesDatabases
          databases={navigationTrees}
          isFetchingDatabases={isFetchingNavigationTrees}
          defaultOpen={() => false}
          onClick={handleDatabaseClick}
        >
          {(database) => (
            <TreesSchemas
              schemas={database.schemas}
              defaultOpen={() => false}
              onClick={handleSchemaClick}
            >
              {(schema) => (
                <TreesTables
                  tables={schema.tables}
                  database={database}
                  schema={schema}
                  defaultOpen={false}
                  onClick={handleTableClick}
                />
              )}
            </TreesSchemas>
          )}
        </TreesDatabases>
      </TreesLayout>
    </>
  );
}
