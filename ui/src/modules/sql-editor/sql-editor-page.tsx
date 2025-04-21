// import { useParams } from '@tanstack/react-router';
import { useState } from 'react';

import { useQueryClient } from '@tanstack/react-query';
import { useParams } from '@tanstack/react-router';
import { EditorCacheProvider } from '@tidbcloud/tisqleditor-react';

// import { useSqlEditorPanelsState } from '@/modules/sql-editor/sql-editor-panels-state-provider';
import type { QueryRecord } from '@/orval/models';
import { getGetQueriesQueryKey, useCreateQuery } from '@/orval/queries';

import { SqlEditorCenterPanel } from './sql-editor-center-panel/sql-editor-center-panel';
import { SqlEditorCenterPanelTabs } from './sql-editor-center-panel/sql-editor-center-panel-tabs';
import { SqlEditorCenterPanelToolbar } from './sql-editor-center-panel/sql-editor-center-panel-toolbar';
import { SqlEditorFooter } from './sql-editor-footer';
import { SqlEditorLeftPanel } from './sql-editor-left-panel/sql-editor-left-panel';
import { SqlEditorRightPanel } from './sql-editor-right-panel/sql-editor-right-panel';

// const DATA: QueryRecord = {
//   durationMs: 0,
//   endTime: '2023-10-01T12:00:05Z',
//   error: 'error',
//   id: 1,
//   query: 'SELECT * FROM users',
//   result: {
//     columns: [
//       { name: 'id', type: 'INT' },
//       { name: 'name', type: 'VARCHAR(255)' },
//     ],
//     rows: [
//       [1, 'John'],
//       [2, 'Charlie'],
//       [3, 'Bob'],
//     ],
//   },
//   resultCount: 0,
//   startTime: '2023-10-01T12:00:00Z',
//   status: 'Ok',
//   worksheetId: 1,
// };

export function SqlEditorPage() {
  const [selectedQueryRecord, setSelectedQueryRecord] = useState<QueryRecord>();
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });

  const queryClient = useQueryClient();

  const {
    data: queryRecord,
    mutateAsync,
    isPending,
    isIdle,
  } = useCreateQuery({
    mutation: {
      onSuccess: () => {
        queryClient.invalidateQueries({
          queryKey: getGetQueriesQueryKey({ worksheetId: +worksheetId }),
        });
      },
    },
  });

  const handleRunQuery = (query: string) => {
    mutateAsync({
      data: {
        query,
        worksheetId: +worksheetId,
      },
    });
  };

  return (
    <div className="relative flex size-full">
      <div className="flex size-full">
        <SqlEditorLeftPanel />

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

        <SqlEditorRightPanel
          selectedQueryRecord={selectedQueryRecord}
          onSetSelectedQueryRecord={setSelectedQueryRecord}
        />
      </div>

      {/* <ResizablePanelGroup direction="horizontal" className="flex flex-grow">
        <SqlEditorResizablePanel
          className="flex flex-grow"
          collapsible
          defaultSize={20}
          maxSize={35}
          minSize={15}
          onCollapse={() => setLeftPanelExpanded(false)}
          onExpand={() => setLeftPanelExpanded(true)}
          order={1}
          ref={leftRef}
        >
          <SqlEditorLeftPanel />
        </SqlEditorResizablePanel>

        <SqlEditorResizableHandle className="mx-3" />

        <SqlEditorResizablePanel className="flex-grow" collapsible defaultSize={60} order={2}>
          <EditorCacheProvider>
            <SqlEditorCenterPanel />
            <SqlEditorFooter />
          </EditorCacheProvider>
        </SqlEditorResizablePanel>

        <SqlEditorResizableHandle className="mx-3" />

        <SqlEditorResizablePanel
          className="flex-grow"
          collapsible
          defaultSize={0}
          maxSize={30}
          minSize={19}
          onCollapse={() => setRightPanelExpanded(false)}
          onExpand={() => setRightPanelExpanded(true)}
          order={3}
          ref={rightRef}
        >
          <SqlEditorRightPanel />
        </SqlEditorResizablePanel>
      </ResizablePanelGroup> */}
    </div>
  );
}
