import { ResizablePanelGroup } from '@/components/ui/resizable';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { SQLEditor } from '@/modules/sql-editor/sql-editor';
import { useSqlEditorPanelsState } from '@/modules/sql-editor/sql-editor-panels-state-provider';
import type { QueryRecord } from '@/orval/models';

import { SqlEditorResizableHandle, SqlEditorResizablePanel } from '../sql-editor-resizable';
import { SqlEditorCenterBottomPanel } from './sql-editor-center-bottom-panel/sql-editor-center-bottom-panel';

interface SQLEditorCenterPanelProps {
  queryRecord?: QueryRecord;
  isLoading?: boolean;
  isIdle?: boolean;
}
export function SqlEditorCenterPanel({
  queryRecord,
  isLoading,
  isIdle,
}: SQLEditorCenterPanelProps) {
  const { groupRef, topRef, bottomRef, setTopPanelExpanded, setBottomPanelExpanded } =
    useSqlEditorPanelsState();

  return (
    <>
      <ResizablePanelGroup direction="vertical" ref={groupRef}>
        <SqlEditorResizablePanel
          collapsible
          defaultSize={30}
          minSize={25}
          onCollapse={() => setTopPanelExpanded(false)}
          onExpand={() => setTopPanelExpanded(true)}
          order={1}
          ref={topRef}
        >
          <ScrollArea className="size-full [&>*>*:first-child]:h-full [&>*>*>*:first-child]:h-full">
            <SQLEditor />
            <ScrollBar orientation="horizontal" />
            <ScrollBar orientation="vertical" />
          </ScrollArea>
        </SqlEditorResizablePanel>

        <SqlEditorResizableHandle />

        <SqlEditorResizablePanel
          collapsible
          defaultSize={70}
          minSize={25}
          onCollapse={() => setBottomPanelExpanded(false)}
          onExpand={() => setBottomPanelExpanded(true)}
          order={2}
          ref={bottomRef}
        >
          <SqlEditorCenterBottomPanel
            queryRecord={queryRecord}
            isLoading={!!isLoading}
            isIdle={!!isIdle}
          />
        </SqlEditorResizablePanel>
      </ResizablePanelGroup>
    </>
  );
}
