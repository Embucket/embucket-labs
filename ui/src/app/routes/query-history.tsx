import { createFileRoute } from '@tanstack/react-router';

import { QueryHistoryPage } from '@/modules/query-history/query-history-page';

export const Route = createFileRoute('/query-history')({
  component: QueryHistoryPage,
});
