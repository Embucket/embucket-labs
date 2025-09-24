import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { SidebarMenu } from '@/components/ui/sidebar';
import type { Worksheet } from '@/orval/models';

import { EditorLeftPanelWorksheet } from './editor-left-panel-worksheet';
import { EditorLeftPanelWorksheetsSkeleton } from './editor-left-panel-worksheets-skeleton';

interface WorksheetsProps {
  worksheets: Worksheet[];
}

function Worksheets({ worksheets }: WorksheetsProps) {
  return worksheets.map((worksheet) => (
    <EditorLeftPanelWorksheet key={worksheet.id} worksheet={worksheet} />
  ));
}

interface EditorLeftPanelWorksheetsProps {
  worksheets: Worksheet[];
  isFetchingWorksheets: boolean;
}

export function EditorLeftPanelWorksheets({
  worksheets,
  isFetchingWorksheets,
}: EditorLeftPanelWorksheetsProps) {
  return (
    // TODO: Hardcode
    <ScrollArea className="h-[calc(100%-56px-2px)] py-2">
      <SidebarMenu className="flex w-full flex-col px-2">
        {isFetchingWorksheets ? (
          <EditorLeftPanelWorksheetsSkeleton />
        ) : (
          <Worksheets worksheets={worksheets} />
        )}
      </SidebarMenu>
      <ScrollBar orientation="vertical" />
    </ScrollArea>
  );
}
