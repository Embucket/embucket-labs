import { createFileRoute } from '@tanstack/react-router';

import { DataPage } from '@/modules/data/data-page';

export const Route = createFileRoute('/data')({
  component: DataPage,
});
