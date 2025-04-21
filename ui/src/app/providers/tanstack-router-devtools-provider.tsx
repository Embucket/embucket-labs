import { lazy, Suspense } from 'react';

const TanStackRouterDevtools = import.meta.env.PROD
  ? () => null
  : lazy(() =>
      import('@tanstack/router-devtools').then((res) => ({
        default: res.TanStackRouterDevtools,
      })),
    );

export function TanStackRouterDevtoolsProvider() {
  return (
    <Suspense>
      <TanStackRouterDevtools />
    </Suspense>
  );
}
