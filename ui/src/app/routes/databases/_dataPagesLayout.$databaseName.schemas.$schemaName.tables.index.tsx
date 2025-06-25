import { createFileRoute } from '@tanstack/react-router';

import { TablesPage } from '@/modules/tables/tables-page';

export const Route = createFileRoute(
  '/databases/_dataPagesLayout/$databaseName/schemas/$schemaName/tables/',
)({
  component: TablesPage,
});
