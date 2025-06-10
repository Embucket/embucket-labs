import { useNavigate, useParams } from '@tanstack/react-router';

import type { SelectedTree } from '@/modules/shared/trees/trees-items';
import {
  TreesDatabases,
  TreesLayout,
  TreesSchemas,
  TreesTables,
} from '@/modules/shared/trees/trees-items';
import { useGetNavigationTrees } from '@/orval/navigation-trees';

export function DataPageTrees() {
  const { databaseName, schemaName, tableName } = useParams({ strict: false });

  const navigate = useNavigate();

  const { data: { items: navigationTrees } = {}, isFetching: isFetchingNavigationTrees } =
    useGetNavigationTrees();

  const handleDatabaseClick = (tree: SelectedTree) => {
    navigate({
      to: '/databases/$databaseName/schemas',
      params: { databaseName: tree.databaseName },
    });
  };

  const handleSchemaClick = (tree: SelectedTree) => {
    if (databaseName) {
      navigate({
        to: '/databases/$databaseName/schemas/$schemaName/tables',
        params: { databaseName: tree.databaseName, schemaName: tree.schemaName },
      });
    }
  };

  const handleTableClick = (tree: SelectedTree) => {
    if (databaseName && schemaName) {
      navigate({
        to: '/databases/$databaseName/schemas/$schemaName/tables/$tableName/columns',
        params: {
          databaseName: tree.databaseName,
          schemaName: tree.schemaName,
          tableName: tree.tableName,
        },
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
              database={database}
              schemas={database.schemas}
              defaultOpen={(schema) => database.name === databaseName && schema.name === schemaName}
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
                      isActive={(view) =>
                        database.name === databaseName &&
                        schema.name === schemaName &&
                        view.name === tableName
                      }
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
