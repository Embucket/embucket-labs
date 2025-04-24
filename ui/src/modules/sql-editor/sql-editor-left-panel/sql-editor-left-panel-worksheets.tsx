import { useEffect } from 'react';

import { Link } from '@tanstack/react-router';
import { Scroll } from 'lucide-react';

import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { SidebarMenu, SidebarMenuButton, SidebarMenuItem } from '@/components/ui/sidebar';
import type { Worksheet } from '@/orval/models';

import { useSqlEditorSettingsStore } from '../sql-editor-settings-store';

interface WorksheetsProps {
  worksheets: Worksheet[];
}

function Worksheets({ worksheets }: WorksheetsProps) {
  const addTab = useSqlEditorSettingsStore((state) => state.addTab);
  const tabs = useSqlEditorSettingsStore((state) => state.tabs);

  useEffect(() => {
    if (worksheets.length && !tabs.length) {
      addTab(worksheets[0]);
    }
  }, [worksheets, addTab, tabs]);

  return (
    <>
      {worksheets.map((worksheet, index) => (
        <SidebarMenuItem key={index}>
          <Link to="/sql-editor/$worksheetId" params={{ worksheetId: worksheet.id.toString() }}>
            {({ isActive }) => (
              <SidebarMenuButton
                onClick={() => addTab(worksheet)}
                className="hover:bg-sidebar-secondary-accent data-[active=true]:bg-sidebar-secondary-accent!"
                isActive={isActive}
              >
                <Scroll />
                <span className="truncate">{worksheet.name}</span>
              </SidebarMenuButton>
            )}
          </Link>
        </SidebarMenuItem>
      ))}
    </>
  );
}

interface SqlEditorLeftPanelWorksheetsProps {
  worksheets: Worksheet[];
}

export function SqlEditorLeftPanelWorksheets({ worksheets }: SqlEditorLeftPanelWorksheetsProps) {
  return (
    <ScrollArea className="h-[calc(100vh-164px)] px-4">
      <SidebarMenu className="flex w-full flex-col">
        <Worksheets worksheets={worksheets} />
      </SidebarMenu>
      <ScrollBar orientation="vertical" />
    </ScrollArea>
  );
}
