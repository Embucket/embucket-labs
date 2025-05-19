import { useNavigate, useParams } from '@tanstack/react-router';

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
  const { databaseName, schemaName, tableName } = useParams({ strict: false });

  const navigate = useNavigate();

  const { data: { items: navigationTrees } = {}, isFetching: isFetchingNavigationTrees } =
    useGetNavigationTrees();

  const handleDatabaseClick = (database: NavigationTreeDatabase) => {
    navigate({ to: '/databases/$databaseName/schemas', params: { databaseName: database.name } });
  };

  const handleSchemaClick = (schema: NavigationTreeSchema) => {
    if (databaseName) {
      navigate({
        to: '/databases/$databaseName/schemas/$schemaName/tables',
        params: { databaseName: databaseName, schemaName: schema.name },
      });
    }
  };

  const handleTableClick = (table: NavigationTreeTable) => {
    if (databaseName && schemaName) {
      navigate({
        to: '/databases/$databaseName/schemas/$schemaName/tables/$tableName/columns',
        params: { databaseName: databaseName, schemaName: schemaName, tableName: table.name },
      });
    }
  };

  return (
    <>
      <TreesLayout scrollAreaClassName="h-[calc(100vh-56px-32px-2px)]">
        <TreesDatabases
          databases={navigationTrees}
          isFetchingDatabases={isFetchingNavigationTrees}
          defaultOpen={(database) => database.name === databaseName}
          isActive={(database) => database.name === databaseName}
          onClick={handleDatabaseClick}
        >
          {(database) => (
            <TreesSchemas
              schemas={database.schemas}
              defaultOpen={(schema) =>
                schema.name === schemaName ||
                schema.tables.some((table) => table.name === tableName)
              }
              isActive={(schema) => schema.name === schemaName}
              onClick={handleSchemaClick}
            >
              {(schema) => (
                <>
                  {!!schema.tables.length && (
                    <TreesTables
                      label="Tables"
                      tables={schema.tables}
                      database={database}
                      isActive={(table) => table.name === tableName}
                      schema={schema}
                      defaultOpen={true}
                      onClick={handleTableClick}
                    />
                  )}
                  {/* TODO: DRY */}
                  {!!schema.views.length && (
                    <TreesTables
                      label="Views"
                      tables={schema.views}
                      database={database}
                      isActive={(table) => table.name === tableName}
                      schema={schema}
                      defaultOpen={true}
                      onClick={handleTableClick}
                    />
                  )}
                </>
              )}
            </TreesSchemas>
          )}
        </TreesDatabases>
      </TreesLayout>
    </>
  );
}
