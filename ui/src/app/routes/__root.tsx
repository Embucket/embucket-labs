import { createRootRouteWithContext, Outlet, redirect, useMatch } from '@tanstack/react-router';
import { Frown } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { SidebarProvider } from '@/components/ui/sidebar';
import type { AuthContextType } from '@/modules/auth/AuthProvider';

import { Layout } from '../layout/layout';
import { AppSidebar } from '../layout/sidebar/app-sidebar';
import { TanStackRouterDevtoolsProvider } from '../providers/tanstack-router-devtools-provider';
import type { FileRoutesByTo } from '../routeTree.gen';

const PUBLIC_PATHS: (keyof FileRoutesByTo)[] = ['/'];

export const Route = createRootRouteWithContext<{
  auth: AuthContextType;
}>()({
  component: Root,
  notFoundComponent: NotFound,
  beforeLoad: ({ location, context }) => {
    if (!context.auth.isAuthenticated) {
      // Redirect to "/" page if not authenticated and trying to access a private route (not in PUBLIC_PATHS)
      if (!PUBLIC_PATHS.includes(location.pathname as keyof FileRoutesByTo)) {
        throw redirect({
          to: '/',
        });
      }
    }
  },
});

function NotFound() {
  return <EmptyContainer Icon={Frown} title="404" description="Ooops. That page doesn't exist." />;
}

interface AuthenticatedLayoutProps {
  children: React.ReactNode;
}

function AuthenticatedLayout({ children }: AuthenticatedLayoutProps) {
  return (
    <SidebarProvider>
      <AppSidebar />
      <Layout>{children}</Layout>
    </SidebarProvider>
  );
}

function Root() {
  const isPublicPage = useMatch({ from: '/', shouldThrow: false });

  if (isPublicPage) {
    return <Outlet />;
  }

  return (
    <AuthenticatedLayout>
      <Outlet />
      <TanStackRouterDevtoolsProvider />
    </AuthenticatedLayout>
  );
}
