import { useEffect, useState } from 'react';

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
import { getGetQueriesQueryKey, useCreateQuery, useGetQuery } from '@/orval/queries';

import { EditorResizableHandle, EditorResizablePanel } from '../editor-resizable';
import { useEditorSettingsStore } from '../editor-settings-store';
import { EditorCenterBottomPanel } from './editor-center-bottom-panel/editor-center-bottom-panel';
import { EditorCenterPanelFooter } from './editor-center-panel-footer';
import { EditorCenterPanelHeader } from './editor-center-panel-header/editor-center-panel-header';
import { EditorCenterPanelToolbar } from './editor-center-panel-toolbar/editor-center-panel-toolbar';

export function EditorCenterPanel() {
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });
  const [pollingQueryId, setPollingQueryId] = useState<number | null>(null);
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

  const { mutate: createQuery } = useCreateQuery({
    mutation: {
      onSuccess: (queryRecord) => {
        if (queryRecord.id) {
          setPollingQueryId(queryRecord.id);
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

  const { data: queryRecord } = useGetQuery(pollingQueryId!, {
    query: {
      enabled: !!pollingQueryId,
      refetchInterval: 500,
      refetchOnWindowFocus: false,
    },
  });

  useEffect(() => {
    if (queryRecord?.status !== 'running') {
      setPollingQueryId(null);

      if (queryRecord?.status === 'successful') {
        if (!isRightPanelExpanded) {
          toggleRightPanel();
        }
        setSelectedQueryRecord(+worksheetId, queryRecord);
      }
      if (queryRecord?.status === 'failed') {
        toast.error(queryRecord.error);
      }
      Promise.all([
        // Use Promise.all since useEffect isn't async
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
    }
  }, [
    isRightPanelExpanded,
    queryClient,
    queryRecord,
    setSelectedQueryRecord,
    toggleRightPanel,
    worksheetId,
  ]);

  const handleRunQuery = (query: string) => {
    createQuery({
      data: {
        query,
        asyncExec: true,
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
            <ScrollArea
              tableViewport
              className="bg-background size-full [&>*>*:first-child]:h-full [&>*>*>*:first-child]:h-full"
            >
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
            <EditorCenterBottomPanel queryRecord={selectedQueryRecord} isLoading={false} />
          </EditorResizablePanel>
        </ResizablePanelGroup>
      </EditorCacheProvider>
      <EditorCenterPanelFooter />
    </div>
  );
}
