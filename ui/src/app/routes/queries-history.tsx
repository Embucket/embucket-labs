import { createFileRoute } from '@tanstack/react-router';

import { QueriesHistoryPage } from '@/modules/queries-history/queries-history-page';

export const Route = createFileRoute('/queries-history')({
  component: QueriesHistoryPage,
});
