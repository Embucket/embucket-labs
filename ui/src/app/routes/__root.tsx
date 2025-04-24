import {
  createRootRouteWithContext,
  Navigate,
  Outlet,
  redirect,
  useMatch,
} from '@tanstack/react-router';

import { SidebarInset, SidebarProvider, useSidebar } from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';
import type { AuthContext } from '@/modules/auth/AuthProvider';
import { SqlEditorPanelsStateProvider } from '@/modules/sql-editor/sql-editor-panels-state-provider';

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

interface LayoutProps {
  children: React.ReactNode;
}

// TODO: Move this to a separate file
function Layout({ children }: LayoutProps) {
  const { open } = useSidebar();

  return (
    <SidebarInset
      className={cn(
        'my-4 max-h-[calc(100vh-16px-16px)]',
        'mr-4 ml-2 w-[calc(100vw-var(--sidebar-width)-16px-8px)]',
      )}
    >
      {
        // TODO: Use css variable
      }
      <div className="relative size-full rounded-lg border bg-[#1F1F1F]">{children}</div>
    </SidebarInset>
  );
}

interface AuthenticatedLayoutProps {
  children: React.ReactNode;
}

function AuthenticatedLayout({ children }: AuthenticatedLayoutProps) {
  return (
    <SidebarProvider>
      <AppSidebar />
      <SqlEditorPanelsStateProvider>
        <Layout>{children}</Layout>
      </SqlEditorPanelsStateProvider>
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
