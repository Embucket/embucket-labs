import { useState } from 'react';

import { Link } from '@tanstack/react-router';
import { MoreHorizontal, Scroll } from 'lucide-react';

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { SidebarMenuAction, SidebarMenuButton, SidebarMenuItem } from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';
import { DeleteWorksheetAlertDialog } from '@/modules/shared/delete-worksheet-alert-dialog/delete-worksheet-alert-dialog';
import type { Worksheet } from '@/orval/models';

import { useEditorSettingsStore } from '../../editor-settings-store';

interface EditorLeftPanelWorksheetProps {
  worksheet: Worksheet;
}

export function EditorLeftPanelWorksheet({ worksheet }: EditorLeftPanelWorksheetProps) {
  const [hoveredWorksheet, setHoveredWorksheet] = useState<Worksheet>();
  const [deleteWorksheetOpened, setDeleteWorksheetOpened] = useState(false);

  const addTab = useEditorSettingsStore((state) => state.addTab);

  return (
    <>
      <SidebarMenuItem>
        <Link to="/sql-editor/$worksheetId" params={{ worksheetId: worksheet.id.toString() }}>
          {({ isActive }) => (
            <SidebarMenuButton
              onMouseEnter={() => setHoveredWorksheet(worksheet)}
              onMouseLeave={() => setHoveredWorksheet(undefined)}
              onClick={() => addTab(worksheet)}
              className="hover:bg-hover data-[active=true]:bg-hover!"
              isActive={isActive}
            >
              <Scroll />
              <span className="truncate">{worksheet.name}</span>
            </SidebarMenuButton>
          )}
        </Link>
        <DropdownMenu>
          <DropdownMenuTrigger
            asChild
            className={cn(
              'invisible group-hover/subitem:visible',
              hoveredWorksheet === worksheet && 'visible',
            )}
          >
            <SidebarMenuAction>
              <MoreHorizontal />
            </SidebarMenuAction>
          </DropdownMenuTrigger>
          <DropdownMenuContent side="right" align="start">
            <DropdownMenuItem
              onClick={() => {
                setHoveredWorksheet(worksheet);
                setDeleteWorksheetOpened(true);
              }}
            >
              <span>Delete worksheet</span>
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </SidebarMenuItem>
      <DeleteWorksheetAlertDialog
        opened={deleteWorksheetOpened}
        onSetOpened={setDeleteWorksheetOpened}
        worksheetId={worksheet.id}
      />
    </>
  );
}
