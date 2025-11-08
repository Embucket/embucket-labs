import type { LinkProps } from '@tanstack/react-router';
import { Link } from '@tanstack/react-router';
import type { LucideIcon } from 'lucide-react';

import {
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from '@/components/ui/sidebar';

interface SidebarNavOption {
  name: string;
  linkProps: LinkProps;
  Icon: LucideIcon;
  disabled?: boolean;
  onClick?: () => void;
  isActive?: boolean;
}

export function AppSidebarGroup({ items, open }: { items: SidebarNavOption[]; open: boolean }) {
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
                href={item.linkProps.href}
                params={item.linkProps.params}
                disabled={item.disabled}
                className="cursor-auto"
              >
                {({ isActive }) => {
                  const actualIsActive = item.isActive ?? isActive;
                  return (
                    <SidebarMenuButton
                      disabled={item.disabled}
                      className="data-[active=true]:text-background data-[active=true]:[&>svg]:text-background text-nowrap"
                      tooltip={{
                        children: item.name,
                        hidden: open,
                      }}
                      isActive={actualIsActive}
                    >
                      <item.Icon />
                      {item.name}
                    </SidebarMenuButton>
                  );
                }}
              </Link>
            </SidebarMenuItem>
          ))}
        </SidebarMenu>
      </SidebarGroupContent>
    </SidebarGroup>
  );
}
