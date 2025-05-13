import { createFileRoute } from '@tanstack/react-router';

import { VolumesPage } from '@/modules/volumes/volumes-page';

export const Route = createFileRoute('/volumes')({
  component: VolumesPage,
});
