import type * as React from 'react';

import { Link } from '@tanstack/react-router';
import {
  BookOpenText,
  Box,
  Database,
  HelpCircle,
  Home,
  PanelRightClose,
  PanelRightOpen,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuItem,
  SidebarTrigger,
  useSidebar,
} from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';
import { useAuth } from '@/modules/auth/AuthProvider';

import { AppSidebarAvatarMenu } from './app-sidebar-avatar-menu';
import { AppSidebarGroup } from './app-sidebar-group';
import { AppSidebarSqlGroup } from './app-sidebar-sql-group';
import { EmbucketLogo } from './logo';

export function AppSidebar({ ...props }: React.ComponentProps<typeof Sidebar>) {
  const { open } = useSidebar();
  const { isAuthenticated } = useAuth();

  return (
    <Sidebar
      variant="floating"
      side="left"
      collapsible="icon"
      className="h-auto rounded-md border-none"
      {...props}
    >
      <SidebarHeader className="flex flex-row items-center justify-between overflow-hidden">
        <Link to={isAuthenticated ? '/home' : '/'}>
          <div className={cn('flex items-center gap-1 transition-all', open && 'ml-2')}>
            <EmbucketLogo />
          </div>
        </Link>
        {open && (
          <Button size="icon" variant="ghost" className="text-muted-foreground size-8" asChild>
            <SidebarTrigger>
              <PanelRightOpen />
            </SidebarTrigger>
          </Button>
        )}
      </SidebarHeader>

      <SidebarContent className="mt-5">
        <AppSidebarGroup
          items={[
            {
              name: 'Home',
              linkProps: {
                to: '/home',
              },
              Icon: Home,
            },
            {
              name: 'Data',
              linkProps: {
                to: '/databases',
              },
              Icon: Database,
            },
            {
              name: 'Volumes',
              linkProps: {
                to: '/volumes',
              },
              Icon: Box,
            },
          ]}
          open={open}
        />
        <AppSidebarSqlGroup />
        <AppSidebarGroup
          items={[
            {
              name: 'Documentation',
              linkProps: {
                href: 'https://docs.embucket.com/essentials/introduction/',
              },
              isActive: false,
              Icon: BookOpenText,
            },
            {
              name: 'Help',
              linkProps: {
                to: '/',
              },
              Icon: HelpCircle,
              disabled: true,
            },
          ]}
          open={open}
        />
        {!open && (
          <SidebarGroup>
            <SidebarGroupContent>
              <SidebarMenu>
                <SidebarMenuItem>
                  <Button
                    size="icon"
                    variant="ghost"
                    className="text-muted-foreground size-8"
                    asChild
                  >
                    <SidebarTrigger>
                      <PanelRightClose />
                    </SidebarTrigger>
                  </Button>
                </SidebarMenuItem>
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        )}
      </SidebarContent>
      <SidebarFooter>
        <AppSidebarAvatarMenu />
      </SidebarFooter>
    </Sidebar>
  );
}
