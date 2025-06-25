import { createFileRoute } from '@tanstack/react-router';

import { DatabasesPage } from '@/modules/databases/databases-page';

export const Route = createFileRoute('/databases/_dataPagesLayout/')({
  component: DatabasesPage,
});
