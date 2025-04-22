import { Link } from '@tanstack/react-router';
import { Scroll } from 'lucide-react';

import { SidebarMenu, SidebarMenuButton, SidebarMenuItem } from '@/components/ui/sidebar';
import type { Worksheet } from '@/orval/models';

interface WorksheetsProps {
  worksheets: Worksheet[];
}

function Worksheets({ worksheets }: WorksheetsProps) {
  return (
    <>
      {worksheets.map((worksheet, index) => (
        <SidebarMenuItem key={index}>
          <Link to="/sql-editor/$worksheetId" params={{ worksheetId: worksheet.id.toString() }}>
            {({ isActive }) => (
              <SidebarMenuButton
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
    <SidebarMenu className="w-full px-4">
      <Worksheets worksheets={worksheets} />
    </SidebarMenu>
  );
}
