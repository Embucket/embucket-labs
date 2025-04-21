import { createFileRoute } from '@tanstack/react-router';

import { HomePage } from '@/modules/home/home-page';

export const Route = createFileRoute('/home')({
  component: HomePage,
});
