import { MoreHorizontal } from 'lucide-react';

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { SidebarMenuAction } from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';

interface SqlEditorLeftPanelTreesTableDropdownProps {
  onLoadDataClick: () => void;
  hovered: boolean;
}

export function SqlEditorLeftPanelTreesTableDropdown({
  onLoadDataClick,
  hovered,
}: SqlEditorLeftPanelTreesTableDropdownProps) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        asChild
        className={cn('invisible group-hover/subitem:visible', hovered && 'visible')}
      >
        <SidebarMenuAction className="size-7">
          <MoreHorizontal />
        </SidebarMenuAction>
      </DropdownMenuTrigger>
      <DropdownMenuContent side="right" align="start">
        <DropdownMenuItem onClick={onLoadDataClick}>
          <span>Load data</span>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
