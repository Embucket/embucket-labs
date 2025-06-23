import { useQueryClient } from '@tanstack/react-query';
import { useParams } from '@tanstack/react-router';
import { EditorCacheProvider } from '@tidbcloud/tisqleditor-react';
import { AxiosError } from 'axios';
import { toast } from 'sonner';

import { ResizablePanelGroup } from '@/components/ui/resizable';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { SQLEditor } from '@/modules/sql-editor/sql-editor';
import { useSqlEditorPanelsState } from '@/modules/sql-editor/sql-editor-panels-state-provider';
import { getGetDashboardQueryKey } from '@/orval/dashboard';
import { getGetNavigationTreesQueryKey } from '@/orval/navigation-trees';
import { getGetQueriesQueryKey, useCreateQuery } from '@/orval/queries';

import { SqlEditorResizableHandle, SqlEditorResizablePanel } from '../sql-editor-resizable';
import { useSqlEditorSettingsStore } from '../sql-editor-settings-store';
import { SqlEditorCenterBottomPanel } from './sql-editor-center-bottom-panel/sql-editor-center-bottom-panel';
import { SqlEditorCenterPanelFooter } from './sql-editor-center-panel-footer';
import { SqlEditorCenterPanelHeader } from './sql-editor-center-panel-header/sql-editor-center-panel-header';
import { SqlEditorCenterPanelToolbar } from './sql-editor-center-panel-toolbar/sql-editor-center-panel-toolbar';

export function SqlEditorCenterPanel() {
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });
  const selectedQueryRecord = useSqlEditorSettingsStore((state) =>
    state.getSelectedQueryRecord(+worksheetId),
  );
  const selectedContext = useSqlEditorSettingsStore((state) => state.selectedContext);
  const setSelectedQueryRecord = useSqlEditorSettingsStore((state) => state.setSelectedQueryRecord);

  const {
    groupRef,
    topRef,
    bottomRef,
    setTopPanelExpanded,
    setBottomPanelExpanded,
    isRightPanelExpanded,
    toggleRightPanel,
  } = useSqlEditorPanelsState();

  const queryClient = useQueryClient();

  const { mutate, isPending } = useCreateQuery({
    mutation: {
      onSettled: async (newQueryRecord) => {
        if (!isRightPanelExpanded) {
          toggleRightPanel();
        }
        await Promise.all([
          queryClient.invalidateQueries({
            queryKey: getGetQueriesQueryKey(),
          }),
          queryClient.invalidateQueries({
            queryKey: getGetDashboardQueryKey(),
          }),
          queryClient.invalidateQueries({
            queryKey: getGetNavigationTreesQueryKey(),
          }),
        ]);
        if (newQueryRecord) {
          setSelectedQueryRecord(+worksheetId, newQueryRecord);
        }
      },
      onError: (error) => {
        if (error instanceof AxiosError) {
          toast.error(error.message, {
            description: error.response?.data.message,
          });
        }
      },
    },
  });

  const handleRunQuery = (query: string) => {
    mutate({
      data: {
        query,
        worksheetId: +worksheetId,
        context: {
          database: selectedContext.database,
          schema: selectedContext.schema,
        },
      },
    });
  };

  return (
    <div className="flex h-full flex-col">
      <SqlEditorCenterPanelHeader />
      <EditorCacheProvider>
        <SqlEditorCenterPanelToolbar onRunQuery={handleRunQuery} />
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
            <ScrollArea
              tableViewport
              className="bg-background size-full [&>*>*:first-child]:h-full [&>*>*>*:first-child]:h-full"
            >
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
            <SqlEditorCenterBottomPanel queryRecord={selectedQueryRecord} isLoading={isPending} />
          </SqlEditorResizablePanel>
        </ResizablePanelGroup>
      </EditorCacheProvider>
      <SqlEditorCenterPanelFooter />
    </div>
  );
}
