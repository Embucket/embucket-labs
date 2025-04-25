import {
  createRootRouteWithContext,
  Navigate,
  Outlet,
  redirect,
  useMatch,
} from '@tanstack/react-router';

import { SidebarProvider } from '@/components/ui/sidebar';
import type { AuthContext } from '@/modules/auth/AuthProvider';

import { Layout } from '../layout/layout';
import { AppSidebar } from '../layout/sidebar/app-sidebar';
// import { TanStackRouterDevtoolsProvider } from '../providers/tanstack-router-devtools-provider';
import type { FileRoutesByTo } from '../routeTree.gen';

const PUBLIC_PATHS: (keyof FileRoutesByTo)[] = ['/'];

export const Route = createRootRouteWithContext<{
  auth: AuthContext;
}>()({
  component: Root,
  notFoundComponent: NotFound,
  beforeLoad: ({ location, context }) => {
    const { pathname } = location;
    if (!context.auth.isAuthenticated) {
      // Redirect to "/" page if not authenticated and trying to access a private route (not in PUBLIC_PATHS)
      if (!PUBLIC_PATHS.includes(pathname as keyof FileRoutesByTo)) {
        throw redirect({
          to: '/',
        });
      }
    } else {
      // Redirect authenticated users from public routes to the home page
      if (PUBLIC_PATHS.includes(pathname as keyof FileRoutesByTo)) {
        throw redirect({
          to: '/home',
        });
      }
    }
  },
});

function NotFound() {
  return <Navigate to="/" />;
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
      {/* <TanStackRouterDevtoolsProvider /> */}
    </AuthenticatedLayout>
  );
}
