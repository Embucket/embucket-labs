import { createFileRoute } from '@tanstack/react-router';

import { SignInPage } from '@/modules/auth/sign-in-page';

export const Route = createFileRoute('/')({
  component: SignInPage,
});
