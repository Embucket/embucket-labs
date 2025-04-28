import { ChevronRight, type LucideIcon } from 'lucide-react';

import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import type { SidebarMenuSubButton } from '@/components/ui/sidebar';
import { SidebarMenuButton, SidebarMenuSub } from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';

interface SqlEditorLeftPanelTreeCollapsibleItemProps {
  icon: LucideIcon;
  label: string;
  triggerComponent?: typeof SidebarMenuButton | typeof SidebarMenuSubButton;
  children: React.ReactNode;
  defaultOpen?: boolean;
  className?: string;
  triggerClassName?: string;
  contentClassName?: string;
}

export function SqlEditorLeftPanelTreeCollapsibleItem({
  icon: Icon,
  label,
  triggerComponent: TriggerComponent = SidebarMenuButton,
  children,
  defaultOpen = true,
  className,
  triggerClassName,
  contentClassName,
}: SqlEditorLeftPanelTreeCollapsibleItemProps) {
  return (
    <Collapsible
      defaultOpen={defaultOpen}
      className={cn(
        'group/collapsible [&[data-state=open]>a>svg:first-child]:rotate-90 [&[data-state=open]>button>svg:first-child]:rotate-90',
        className,
      )}
    >
      <CollapsibleTrigger asChild>
        <TriggerComponent className={cn('hover:bg-sidebar-secondary-accent', triggerClassName)}>
          <ChevronRight className="transition-transform" />
          <Icon />
          <span className="truncate">{label}</span>
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
