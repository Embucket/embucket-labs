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

export function DatabasesPageTrees() {
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
      <TreesLayout scrollAreaClassName="h-[calc(100vh-56px-32px-2px)]">
        <TreesDatabases
          databases={navigationTrees}
          isFetchingDatabases={isFetchingNavigationTrees}
          defaultOpen={() => true}
          onClick={handleDatabaseClick}
        >
          {(database) => (
            <TreesSchemas
              schemas={database.schemas}
              defaultOpen={() => true}
              onClick={handleSchemaClick}
            >
              {(schema) => (
                <TreesTables
                  tables={schema.tables}
                  database={database}
                  schema={schema}
                  defaultOpen={true}
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
