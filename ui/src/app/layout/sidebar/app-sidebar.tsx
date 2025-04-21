import type * as React from 'react';

import type { LinkProps } from '@tanstack/react-router';
import { Link } from '@tanstack/react-router';
import type { LucideIcon } from 'lucide-react';
import {
  Activity,
  BookOpenText,
  Database,
  HelpCircle,
  Home,
  PanelRightClose,
  PanelRightOpen,
  SquareTerminal,
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
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarTrigger,
  useSidebar,
} from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';

import { AppSidebarAvatarMenu } from './app-sidebar-avatar-menu';
import { AppSidebarOrgMenu } from './app-sidebar-org-menu';
import { EmbucketLogo, EmbucketLogoText } from './logo';

interface SidebarNavOption {
  name: string;
  linkProps: LinkProps;
  options?: SidebarNavOption[];
  Icon: LucideIcon;
  disabled?: boolean;
}

const sidebarNavItems: SidebarNavOption[] = [
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
      to: '/data',
    },
    Icon: Database,
  },
  {
    name: 'Query History',
    linkProps: {
      to: '/query-history',
    },
    Icon: Activity,
  },
  {
    name: 'Sql Editor',
    linkProps: {
      to: '/sql-editor/$worksheetId',
      params: {
        worksheetId: 'undefined',
      },
    },
    Icon: SquareTerminal,
  },
];

const helpNavItems: SidebarNavOption[] = [
  {
    name: 'Documentation',
    linkProps: {
      to: '/',
    },
    Icon: BookOpenText,
    disabled: true,
  },
  {
    name: 'Help',
    linkProps: {
      to: '/',
    },
    Icon: HelpCircle,
    disabled: true,
  },
];

export function AppSidebar({ ...props }: React.ComponentProps<typeof Sidebar>) {
  const { open } = useSidebar();

  return (
    <Sidebar side="left" collapsible="icon" className="bg-muted h-auto rounded-md" {...props}>
      <SidebarHeader className="flex flex-row items-center justify-between pt-4">
        <Link to="/">
          <div className={cn('flex items-center gap-1 transition-all', open && 'ml-2')}>
            <EmbucketLogo />
            {open && <EmbucketLogoText />}
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
      <SidebarHeader className="py-2">
        <AppSidebarOrgMenu />
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent className="px-0">
            <SidebarMenu>
              {sidebarNavItems.map((item) => (
                <SidebarMenuItem key={item.name}>
                  <Link to={item.linkProps.to} className="cursor-auto">
                    {({ isActive }) => (
                      <SidebarMenuButton
                        disabled={item.disabled}
                        className={cn(
                          !isActive && 'hover:bg-sidebar-secondary-accent!',
                          'text-nowrap',
                        )}
                        tooltip={{
                          children: item.name,
                          hidden: open,
                        }}
                        isActive={isActive}
                      >
                        <item.Icon />
                        {item.name}
                      </SidebarMenuButton>
                    )}
                  </Link>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
        <SidebarGroup>
          <SidebarGroupContent className="px-0">
            <SidebarMenu>
              {helpNavItems.map((item) => (
                <SidebarMenuItem key={item.name}>
                  <Link to={item.linkProps.to} className="cursor-auto">
                    {({ isActive }) => (
                      <SidebarMenuButton
                        disabled={item.disabled}
                        className={cn(
                          !isActive && 'hover:bg-sidebar-secondary-accent!',
                          'text-nowrap',
                        )}
                        tooltip={{
                          children: item.name,
                          hidden: false,
                        }}
                        isActive={isActive}
                      >
                        <item.Icon />
                        {item.name}
                      </SidebarMenuButton>
                    )}
                  </Link>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
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
        <AppSidebarAvatarMenu
          user={{
            name: 'John Doe',
            email: 'johndoe@gmail.com',
            avatar: 'https://ui.shadcn.com/avatars/04.png',
          }}
        />
      </SidebarFooter>
    </Sidebar>
  );
}
