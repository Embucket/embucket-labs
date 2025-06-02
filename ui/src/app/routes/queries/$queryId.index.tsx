import { createFileRoute } from '@tanstack/react-router';

import { QueryPage } from '@/modules/query/query-page';

export const Route = createFileRoute('/queries/$queryId/')({
  component: QueryPage,
});
