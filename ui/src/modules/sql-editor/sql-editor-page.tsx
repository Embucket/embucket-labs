import { ResizablePanelGroup } from '@/components/ui/resizable';

import { SqlEditorCenterPanel } from './sql-editor-center-panel/sql-editor-center-panel';
import { SqlEditorLeftPanel } from './sql-editor-left-panel/sql-editor-left-panel';
import { useSqlEditorPanelsState } from './sql-editor-panels-state-provider';
import { SqlEditorResizableHandle, SqlEditorResizablePanel } from './sql-editor-resizable';
import { SqlEditorRightPanel } from './sql-editor-right-panel/sql-editor-right-panel';
import { useSyncSqlEditorSelectedTree } from './use-sync-sql-editor-selected-tree';
import { useSyncSqlEditorTabs } from './use-sync-sql-editor-tabs';

export function SqlEditorPage() {
  const { leftRef, rightRef, setLeftPanelExpanded, setRightPanelExpanded } =
    useSqlEditorPanelsState();

  useSyncSqlEditorTabs();
  useSyncSqlEditorSelectedTree();

  return (
    <>
      <ResizablePanelGroup direction="horizontal">
        <SqlEditorResizablePanel
          collapsible
          defaultSize={20}
          maxSize={30}
          minSize={20}
          onCollapse={() => setLeftPanelExpanded(false)}
          onExpand={() => setLeftPanelExpanded(true)}
          order={1}
          ref={leftRef}
        >
          <SqlEditorLeftPanel />
        </SqlEditorResizablePanel>

        <SqlEditorResizableHandle />

        <SqlEditorResizablePanel collapsible defaultSize={60} order={2}>
          <SqlEditorCenterPanel />
        </SqlEditorResizablePanel>

        <SqlEditorResizableHandle />

        <SqlEditorResizablePanel
          collapsible
          defaultSize={20}
          maxSize={30}
          minSize={20}
          onCollapse={() => setRightPanelExpanded(false)}
          onExpand={() => setRightPanelExpanded(true)}
          order={3}
          ref={rightRef}
        >
          <SqlEditorRightPanel />
        </SqlEditorResizablePanel>
      </ResizablePanelGroup>
    </>
  );
}
