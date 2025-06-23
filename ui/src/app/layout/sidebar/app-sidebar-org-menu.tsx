import { Building2, ChevronsUpDown } from 'lucide-react';

import { Avatar } from '@/components/ui/avatar';
import { DropdownMenu, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { SidebarMenu, SidebarMenuButton, SidebarMenuItem } from '@/components/ui/sidebar';

export function AppSidebarOrgMenu() {
  return (
    <SidebarMenu>
      <SidebarMenuItem>
        <DropdownMenu>
          <DropdownMenuTrigger className="bg-background-secondary" asChild>
            <SidebarMenuButton size="lg">
              <Avatar className="flex h-8 w-8 items-center justify-center rounded-lg">
                <Building2 className="size-4" />
              </Avatar>
              <div className="grid flex-1 text-left text-sm leading-tight group-data-[collapsible=icon]:hidden">
                <span className="truncate font-semibold">Booster Inc.</span>
                <span className="text-muted-foreground truncate text-xs">Premium</span>
              </div>
              <ChevronsUpDown className="ml-auto size-4 group-data-[collapsible=icon]:hidden" />
            </SidebarMenuButton>
          </DropdownMenuTrigger>
        </DropdownMenu>
      </SidebarMenuItem>
    </SidebarMenu>
  );
}
