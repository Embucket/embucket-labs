import { useState } from 'react';

import { useQueryClient } from '@tanstack/react-query';
import { useParams } from '@tanstack/react-router';
import { EditorCacheProvider } from '@tidbcloud/tisqleditor-react';

import { ResizablePanelGroup } from '@/components/ui/resizable';
import { getGetDashboardQueryKey } from '@/orval/dashboard';
import type { QueryRecord } from '@/orval/models';
import { getGetQueriesQueryKey, useCreateQuery } from '@/orval/queries';

import { SqlEditorCenterPanel } from './sql-editor-center-panel/sql-editor-center-panel';
import { SqlEditorCenterPanelTabs } from './sql-editor-center-panel/sql-editor-center-panel-tabs';
import { SqlEditorCenterPanelToolbar } from './sql-editor-center-panel/sql-editor-center-panel-toolbar';
import { SqlEditorFooter } from './sql-editor-footer';
import { SqlEditorLeftPanel } from './sql-editor-left-panel/sql-editor-left-panel';
import { useSqlEditorPanelsState } from './sql-editor-panels-state-provider';
import { SqlEditorResizableHandle, SqlEditorResizablePanel } from './sql-editor-resizable';
import { SqlEditorRightPanel } from './sql-editor-right-panel/sql-editor-right-panel';

export function SqlEditorPage() {
  const [selectedQueryRecord, setSelectedQueryRecord] = useState<QueryRecord>();
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });
  const {
    isRightPanelExpanded,
    toggleRightPanel,
    leftRef,
    rightRef,
    setLeftPanelExpanded,
    setRightPanelExpanded,
  } = useSqlEditorPanelsState();

  const queryClient = useQueryClient();

  const {
    data: queryRecord,
    mutateAsync,
    isPending,
    isIdle,
  } = useCreateQuery({
    mutation: {
      onSuccess: async () => {
        await Promise.all([
          queryClient.invalidateQueries({
            queryKey: getGetQueriesQueryKey({ worksheetId: +worksheetId }),
          }),
          queryClient.invalidateQueries({
            queryKey: getGetDashboardQueryKey(),
          }),
        ]);
      },
    },
  });

  const handleRunQuery = async (query: string) => {
    await mutateAsync({
      data: {
        query,
        worksheetId: +worksheetId,
      },
    });
    if (!isRightPanelExpanded) {
      toggleRightPanel();
    }
  };

  return (
    <>
      <ResizablePanelGroup direction="horizontal">
        <SqlEditorResizablePanel
          collapsible
          defaultSize={20}
          maxSize={40}
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
          <div className="flex size-full flex-col">
            <SqlEditorCenterPanelTabs />
            <EditorCacheProvider>
              <SqlEditorCenterPanelToolbar onRunQuery={handleRunQuery} />
              <SqlEditorCenterPanel
                queryRecord={selectedQueryRecord ?? queryRecord}
                isLoading={isPending}
                isIdle={isIdle}
              />
            </EditorCacheProvider>
            <SqlEditorFooter />
          </div>
        </SqlEditorResizablePanel>

        <SqlEditorResizableHandle />

        <SqlEditorResizablePanel
          collapsible
          defaultSize={20}
          maxSize={40}
          minSize={20}
          onCollapse={() => setRightPanelExpanded(false)}
          onExpand={() => setRightPanelExpanded(true)}
          order={3}
          ref={rightRef}
        >
          <SqlEditorRightPanel
            selectedQueryRecord={selectedQueryRecord}
            onSetSelectedQueryRecord={setSelectedQueryRecord}
          />
        </SqlEditorResizablePanel>
      </ResizablePanelGroup>
    </>
  );
}
