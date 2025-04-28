import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { SidebarMenu } from '@/components/ui/sidebar';
import type { Worksheet } from '@/orval/models';

import { SqlEditorLeftPanelWorksheet } from './sql-editor-left-panel-worksheet';

interface WorksheetsProps {
  worksheets: Worksheet[];
}

function Worksheets({ worksheets }: WorksheetsProps) {
  return worksheets.map((worksheet) => (
    <SqlEditorLeftPanelWorksheet key={worksheet.id} worksheet={worksheet} />
  ));
}

interface SqlEditorLeftPanelWorksheetsProps {
  worksheets: Worksheet[];
}

export function SqlEditorLeftPanelWorksheets({ worksheets }: SqlEditorLeftPanelWorksheetsProps) {
  return (
    // TODO: Hardcode
    <ScrollArea className="h-[calc(100%-48px)] py-2">
      <SidebarMenu className="flex w-full flex-col px-2">
        <Worksheets worksheets={worksheets} />
      </SidebarMenu>
      <ScrollBar orientation="vertical" />
    </ScrollArea>
  );
}
