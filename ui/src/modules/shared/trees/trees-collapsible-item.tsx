import type { ReactNode } from 'react';

import { ChevronRight, type LucideIcon } from 'lucide-react';

import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import type { SidebarMenuSubButton } from '@/components/ui/sidebar';
import { SidebarMenuButton, SidebarMenuSub } from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';

interface TreeCollapsibleItemProps {
  icon: LucideIcon;
  label: ReactNode;
  triggerComponent?: typeof SidebarMenuButton | typeof SidebarMenuSubButton;
  children: React.ReactNode;
  defaultOpen?: boolean;
  isActive?: boolean;
  className?: string;
  open?: boolean;
  triggerClassName?: string;
  contentClassName?: string;
  onClick?: () => void;
  disabled?: boolean;
}

export function TreeCollapsibleItem({
  icon: Icon,
  label,
  triggerComponent: TriggerComponent = SidebarMenuButton,
  children,
  defaultOpen = true,
  isActive,
  className,
  open,
  triggerClassName,
  contentClassName,
  onClick,
  disabled = false,
}: TreeCollapsibleItemProps) {
  return (
    <Collapsible
      defaultOpen={defaultOpen}
      open={open}
      className={cn(
        'group/collapsible [&[data-state=open]>a>svg:first-child]:rotate-90 [&[data-state=open]>button>svg:first-child]:rotate-90',
        className,
      )}
    >
      <CollapsibleTrigger asChild>
        <TriggerComponent
          className={cn(
            'hover:bg-sidebar-secondary-accent data-[active=true]:bg-sidebar-secondary-accent!',
            triggerClassName,
          )}
          onClick={onClick}
          isActive={isActive}
          disabled={disabled}
        >
          <ChevronRight className="transition-transform" />
          <Icon />
          {typeof label === 'string' ? <span className="truncate">{label}</span> : label}
        </TriggerComponent>
      </CollapsibleTrigger>
      <CollapsibleContent>
        <SidebarMenuSub className={cn('mr-0 pr-0 pb-0', contentClassName)}>
          {children}
        </SidebarMenuSub>
      </CollapsibleContent>
    </Collapsible>
  );
}
