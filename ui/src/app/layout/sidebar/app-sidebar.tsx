import type * as React from 'react';

import { useQueryClient } from '@tanstack/react-query';
import type { LinkProps } from '@tanstack/react-router';
import { Link, useLocation, useNavigate } from '@tanstack/react-router';
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
import { useSqlEditorSettingsStore } from '@/modules/sql-editor/sql-editor-settings-store';
import { getGetWorksheetsQueryKey, useCreateWorksheet, useGetWorksheets } from '@/orval/worksheets';

import { AppSidebarAvatarMenu } from './app-sidebar-avatar-menu';
import { AppSidebarOrgMenu } from './app-sidebar-org-menu';
import { EmbucketLogo, EmbucketLogoText } from './logo';

interface SidebarNavOption {
  name: string;
  linkProps: LinkProps;
  Icon: LucideIcon;
  disabled?: boolean;
  onClick?: () => void;
  isActive?: boolean;
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
      to: '/databases',
    },
    Icon: Database,
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

function SidebarGroupComponent({ items, open }: { items: SidebarNavOption[]; open: boolean }) {
  return (
    <SidebarGroup>
      <SidebarGroupContent className="px-0">
        <SidebarMenu>
          {items.map((item) => (
            <SidebarMenuItem key={item.name}>
              <Link
                onClick={(e) => {
                  if (item.onClick) {
                    e.preventDefault();
                    item.onClick();
                  }
                }}
                to={item.linkProps.to}
                params={item.linkProps.params}
                disabled={item.disabled}
                className="cursor-auto"
              >
                {({ isActive }) => (
                  <SidebarMenuButton
                    disabled={item.disabled}
                    className={cn(!isActive && 'hover:bg-sidebar-secondary-accent!', 'text-nowrap')}
                    tooltip={{
                      children: item.name,
                      hidden: open,
                    }}
                    isActive={isActive || item.isActive}
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
  );
}

export function AppSidebar({ ...props }: React.ComponentProps<typeof Sidebar>) {
  const { open } = useSidebar();
  const { data: { items: worksheets } = {}, isFetching: isFetchingWorksheets } = useGetWorksheets();
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const { pathname } = useLocation();

  const addTab = useSqlEditorSettingsStore((state) => state.addTab);

  const { mutateAsync, isPending } = useCreateWorksheet({
    mutation: {
      onSuccess: (worksheet) => {
        queryClient.invalidateQueries({
          queryKey: getGetWorksheetsQueryKey(),
        });
        addTab(worksheet);
        navigate({
          to: '/sql-editor/$worksheetId',
          params: {
            worksheetId: worksheet.id.toString(),
          },
        });
      },
    },
  });

  const handleCreateWorksheet = () => {
    if (!worksheets?.length) {
      mutateAsync({
        data: {
          name: '',
          content: '',
        },
      });
      return;
    }
    addTab(worksheets[0]);
    navigate({
      to: '/sql-editor/$worksheetId',
      params: {
        worksheetId: worksheets[0]?.id.toString(),
      },
    });
  };

  return (
    <Sidebar
      variant="floating"
      side="left"
      collapsible="icon"
      className="h-auto rounded-md border-none"
      {...props}
    >
      <SidebarHeader className="flex flex-row items-center justify-between">
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
        <SidebarGroupComponent items={sidebarNavItems} open={open} />
        <SidebarGroupComponent
          items={[
            {
              name: 'SQL',
              linkProps: {
                to: '/sql-editor/$worksheetId',
                params: {
                  worksheetId: worksheets?.[0]?.id.toString(),
                },
              },
              Icon: SquareTerminal,
              disabled: isFetchingWorksheets || isPending,
              onClick: handleCreateWorksheet,
              isActive: pathname.includes('/sql-editor'),
            },
            {
              name: 'Queries',
              linkProps: {
                to: '/queries-history',
              },
              Icon: Activity,
            },
          ]}
          open={open}
        />
        <SidebarGroupComponent items={helpNavItems} open={open} />
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
