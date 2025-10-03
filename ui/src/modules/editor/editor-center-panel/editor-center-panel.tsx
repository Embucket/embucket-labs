import { useQueryClient } from '@tanstack/react-query';
import { useParams } from '@tanstack/react-router';
import { EditorCacheProvider } from '@tidbcloud/tisqleditor-react';
import { AxiosError } from 'axios';
import { toast } from 'sonner';

import { ResizablePanelGroup } from '@/components/ui/resizable';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { Editor } from '@/modules/editor/editor';
import { useEditorPanelsState } from '@/modules/editor/editor-panels-state-provider';
import { getGetDashboardQueryKey } from '@/orval/dashboard';
import { getGetNavigationTreesQueryKey } from '@/orval/navigation-trees';
import { getGetQueriesQueryKey, useCreateQuery } from '@/orval/queries';

import { EditorResizableHandle, EditorResizablePanel } from '../editor-resizable';
import { useEditorSettingsStore } from '../editor-settings-store';
import { EditorCenterBottomPanel } from './editor-center-bottom-panel/editor-center-bottom-panel';
import { EditorCenterPanelFooter } from './editor-center-panel-footer';
import { EditorCenterPanelHeader } from './editor-center-panel-header/editor-center-panel-header';
import { EditorCenterPanelToolbar } from './editor-center-panel-toolbar/editor-center-panel-toolbar';

export function EditorCenterPanel() {
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });
  const selectedQueryRecord = useEditorSettingsStore((state) =>
    state.getSelectedQueryRecord(+worksheetId),
  );
  const selectedContext = useEditorSettingsStore((state) => state.selectedContext);
  const setSelectedQueryRecord = useEditorSettingsStore((state) => state.setSelectedQueryRecord);

  const {
    groupRef,
    topRef,
    bottomRef,
    setTopPanelExpanded,
    setBottomPanelExpanded,
    isRightPanelExpanded,
    toggleRightPanel,
  } = useEditorPanelsState();

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
      <EditorCenterPanelHeader />
      <EditorCacheProvider>
        <EditorCenterPanelToolbar onRunQuery={handleRunQuery} />
        <ResizablePanelGroup direction="vertical" ref={groupRef}>
          <EditorResizablePanel
            collapsible
            defaultSize={30}
            minSize={25}
            onCollapse={() => setTopPanelExpanded(false)}
            onExpand={() => setTopPanelExpanded(true)}
            order={1}
            ref={topRef}
          >
            <ScrollArea className="bg-background size-full [&>*>*:first-child]:h-full [&>*>*>*:first-child]:h-full">
              <Editor />
              <ScrollBar orientation="horizontal" />
              <ScrollBar orientation="vertical" />
            </ScrollArea>
          </EditorResizablePanel>

          <EditorResizableHandle />

          <EditorResizablePanel
            collapsible
            defaultSize={70}
            minSize={25}
            onCollapse={() => setBottomPanelExpanded(false)}
            onExpand={() => setBottomPanelExpanded(true)}
            order={2}
            ref={bottomRef}
          >
            <EditorCenterBottomPanel queryRecord={selectedQueryRecord} isLoading={isPending} />
          </EditorResizablePanel>
        </ResizablePanelGroup>
      </EditorCacheProvider>
      <EditorCenterPanelFooter />
    </div>
  );
}
