import { createRouter, RouterProvider } from '@tanstack/react-router';

import { routeTree } from '@/app/routeTree.gen';
import { useAuth } from '@/modules/auth/AuthProvider';

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}

const router = createRouter({
  routeTree,
  // https://github.com/TanStack/router/issues/2183
  // defaultPreload: 'intent',
  // defaultPendingMinMs: 0,
  context: {
    // auth will initially be undefined
    // We'll be passing down the auth state from within a React component
    auth: undefined!,
  },
});

export const ReactRouterProvider = () => {
  const auth = useAuth();

  return (
    <RouterProvider
      router={router}
      context={{
        auth,
      }}
    />
  );
};
