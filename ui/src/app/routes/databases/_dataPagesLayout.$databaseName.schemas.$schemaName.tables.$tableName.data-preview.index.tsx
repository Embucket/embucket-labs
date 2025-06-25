import { createFileRoute } from '@tanstack/react-router';

import { DataPreviewPage } from '@/modules/data-preview/data-preview-page';

export const Route = createFileRoute(
  '/databases/_dataPagesLayout/$databaseName/schemas/$schemaName/tables/$tableName/data-preview/',
)({
  component: DataPreviewPage,
});
