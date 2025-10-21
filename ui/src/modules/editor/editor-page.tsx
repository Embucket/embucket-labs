import { EditorCacheProvider } from '@tidbcloud/tisqleditor-react';

import { ResizablePanelGroup } from '@/components/ui/resizable';

import { EditorCenterPanel } from './editor-center-panel/editor-center-panel';
import { EditorLeftPanel } from './editor-left-panel/editor-left-panel';
import { useEditorPanelsState } from './editor-panels-state-provider';
import { EditorResizableHandle, EditorResizablePanel } from './editor-resizable';
import { EditorRightPanel } from './editor-right-panel/editor-right-panel';
import { useSyncEditorSelectedTree } from './use-sync-editor-selected-tree';
import { useSyncEditorTabs } from './use-sync-editor-tabs';

export function EditorPage() {
  const { leftRef, rightRef, setLeftPanelExpanded, setRightPanelExpanded } = useEditorPanelsState();

  useSyncEditorTabs();
  useSyncEditorSelectedTree();

  return (
    <>
      <ResizablePanelGroup direction="horizontal">
        <EditorResizablePanel
          collapsible
          defaultSize={20}
          maxSize={30}
          minSize={20}
          onCollapse={() => setLeftPanelExpanded(false)}
          onExpand={() => setLeftPanelExpanded(true)}
          order={1}
          ref={leftRef}
        >
          <EditorLeftPanel />
        </EditorResizablePanel>

        <EditorResizableHandle />

        <EditorResizablePanel collapsible defaultSize={60} order={2}>
          <EditorCacheProvider>
            <EditorCenterPanel />
          </EditorCacheProvider>
        </EditorResizablePanel>

        <EditorResizableHandle />

        <EditorResizablePanel
          collapsible
          defaultSize={20}
          maxSize={30}
          minSize={20}
          onCollapse={() => setRightPanelExpanded(false)}
          onExpand={() => setRightPanelExpanded(true)}
          order={3}
          ref={rightRef}
        >
          <EditorRightPanel />
        </EditorResizablePanel>
      </ResizablePanelGroup>
    </>
  );
}
