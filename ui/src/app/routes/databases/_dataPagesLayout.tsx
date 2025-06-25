import { createFileRoute } from '@tanstack/react-router';

import { DataPagesLayout } from '@/modules/shared/data-page/data-page-layout';

export const Route = createFileRoute('/databases/_dataPagesLayout')({
  component: DataPagesLayout,
});
