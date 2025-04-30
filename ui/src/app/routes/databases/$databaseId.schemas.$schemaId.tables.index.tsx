import { createFileRoute } from '@tanstack/react-router';

import { TablesPage } from '@/modules/tables/tables-page';

export const Route = createFileRoute('/databases/$databaseId/schemas/$schemaId/tables/')({
  component: TablesPage,
});
