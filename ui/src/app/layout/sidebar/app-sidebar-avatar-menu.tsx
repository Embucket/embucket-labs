import { useNavigate } from '@tanstack/react-router';
import { ChevronsUpDown, LogOut, Settings } from 'lucide-react';

import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { SidebarMenu, SidebarMenuButton, SidebarMenuItem } from '@/components/ui/sidebar';
import { useAuth } from '@/modules/auth/AuthProvider';
import { useGetAccount } from '@/orval/account';
import { useLogout } from '@/orval/auth';

const USER = {
  email: 'johndoe@embucket.com',
  avatar: 'https://ui.shadcn.com/avatars/04.png',
};

export function AppSidebarAvatarMenu() {
  const { resetAuthenticated } = useAuth();
  const navigate = useNavigate();

  const { data: account, isPending: isAccountPending } = useGetAccount();

  const { mutate: logout, isPending: isLogoutPending } = useLogout({
    mutation: {
      onSuccess: () => {
        resetAuthenticated();
        navigate({
          to: '/',
        });
      },
    },
  });

  const handleLogout = () => {
    logout();
  };

  const userName = account?.username ?? '';

  return (
    <SidebarMenu>
      <SidebarMenuItem>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <SidebarMenuButton size="lg" disabled={isAccountPending}>
              <Avatar className="h-8 w-8 rounded-lg hover:brightness-120">
                <AvatarImage src={USER.avatar} alt={userName} />
                <AvatarFallback className="rounded-lg">CN</AvatarFallback>
              </Avatar>
              <div className="grid flex-1 text-left text-sm leading-tight group-data-[collapsible=icon]:hidden">
                <span className="truncate font-semibold capitalize">{userName}</span>
                <span className="text-muted-foreground truncate text-xs">{USER.email}</span>
              </div>
              <ChevronsUpDown className="ml-auto size-4 group-data-[collapsible=icon]:hidden" />
            </SidebarMenuButton>
          </DropdownMenuTrigger>
          <DropdownMenuContent
            className="w-[--radix-dropdown-menu-trigger-width] min-w-56 rounded-lg"
            side="right"
            align="end"
            sideOffset={4}
          >
            <DropdownMenuLabel className="p-0 font-normal">
              <div className="flex items-center gap-2 px-1 py-1.5 text-left text-sm">
                <Avatar className="h-8 w-8 rounded-lg">
                  <AvatarImage src={USER.avatar} alt={userName} />
                  <AvatarFallback className="rounded-lg">CN</AvatarFallback>
                </Avatar>
                <div className="grid flex-1 text-left text-sm leading-tight">
                  <span className="truncate font-semibold capitalize">{userName}</span>
                  <span className="truncate text-xs">{USER.email}</span>
                </div>
              </div>
            </DropdownMenuLabel>
            <DropdownMenuSeparator />
            <DropdownMenuGroup>
              <DropdownMenuItem>
                <Settings />
                Account settings
              </DropdownMenuItem>
            </DropdownMenuGroup>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={handleLogout} disabled={isLogoutPending}>
              <LogOut />
              Log out
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </SidebarMenuItem>
    </SidebarMenu>
  );
}
