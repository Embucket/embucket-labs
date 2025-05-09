import { createFileRoute, redirect } from '@tanstack/react-router';

import { SignInPage } from '@/modules/auth/sign-in-page';

export const Route = createFileRoute('/')({
  component: SignInPage,
  beforeLoad: ({ context }) => {
    if (context.auth.isAuthenticated) {
      throw redirect({
        to: '/home',
      });
    }
  },
});
