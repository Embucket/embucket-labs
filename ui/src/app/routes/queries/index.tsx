import { createFileRoute } from '@tanstack/react-router';

import { QueriesHistoryPage } from '@/modules/queries/queries-history-page';

export const Route = createFileRoute('/queries/')({
  component: QueriesHistoryPage,
});
