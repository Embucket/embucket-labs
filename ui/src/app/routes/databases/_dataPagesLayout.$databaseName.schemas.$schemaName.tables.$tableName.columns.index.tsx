import { createFileRoute } from '@tanstack/react-router';

import { ColumnsPage } from '@/modules/columns/columns-page';

export const Route = createFileRoute(
  '/databases/_dataPagesLayout/$databaseName/schemas/$schemaName/tables/$tableName/columns/',
)({
  component: ColumnsPage,
});
