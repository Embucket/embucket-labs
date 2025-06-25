import { createFileRoute } from '@tanstack/react-router';

import { SchemasPage } from '@/modules/schemas/schemas-page';

export const Route = createFileRoute('/databases/_dataPagesLayout/$databaseName/schemas/')({
  component: SchemasPage,
});
