import { useRef, useState } from 'react';

import { AlertTriangle, ArrowDownToLine, Search, TextSearch } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { Button } from '@/components/ui/button';
import { ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import type { QueryRecord } from '@/orval/models';

import { SqlEditorResizableHandle } from '../../sql-editor-resizable';
import { SqlEditorCenterBottomPanelQueryResultTable } from './sql-editor-center-bottom-panel-query-result-table';
import { SqlEditorCenterBottomPanelSelectedCell } from './sql-editor-center-bottom-panel-selected-cell.tsx';
import { useEditorScrollToSelectedCell } from './use-editor-scroll-to-selected-cell.tsx';

interface SqlEditorCenterPanelQueryColumnsProps {
  isLoading: boolean;
  queryRecord?: QueryRecord;
}

export function SqlEditorCenterBottomPanel({
  isLoading,
  queryRecord,
}: SqlEditorCenterPanelQueryColumnsProps) {
  const [selectedCellId, setSelectedCellId] = useState<string>();
  const scrollRootRef = useRef<HTMLDivElement | null>(null);

  useEditorScrollToSelectedCell({ selectedCellId, scrollRootRef });

  if (queryRecord?.error) {
    // TODO: EmptyContainer designed to be used for empty states strictly
    return (
      <EmptyContainer
        Icon={AlertTriangle}
        title="Query failed"
        description={
          <span className="text-sm whitespace-pre-wrap text-red-500">{queryRecord.error}</span>
        }
      />
    );
  }

  const columns = queryRecord?.result.columns ?? [];
  const rows = queryRecord?.result.rows ?? [];
  const noFields = !columns.length && !isLoading;

  const rowCount = rows.length.toString();
  const columnCount = columns.length.toString();
  const executionTime = queryRecord ? queryRecord.durationMs / 1000 : 0; // Convert ms to seconds

  return (
    <>
      {!queryRecord && (
        <EmptyContainer
          Icon={TextSearch}
          title="No Results Yet"
          description="Once you run a query, results will be displayed here."
        />
      )}

      {!noFields && (
        <Tabs defaultValue="results" variant="underline" className="size-full">
          <TabsList className="px-4">
            <TabsTrigger value="results">Results</TabsTrigger>
            {/* <TabsTrigger disabled value="chart">
              Chart
            </TabsTrigger> */}
          </TabsList>
          <div className="flex items-center px-4 py-2">
            {!isLoading && (
              <div className="text-muted-foreground flex text-xs">
                {`${rowCount} Rows x ${columnCount} Columns processed in ${executionTime.toFixed(3)}s`}
              </div>
            )}

            <div className="ml-auto flex items-center gap-2">
              <Button disabled size="icon" variant="ghost" className="text-muted-foreground size-8">
                <Search />
              </Button>
              <Button disabled size="icon" variant="ghost" className="text-muted-foreground size-8">
                <ArrowDownToLine />
              </Button>
            </div>
          </div>
          {/* TODO: Hardcode */}
          <TabsContent value="results" className="m-0 h-[calc(100%-100px)]">
            <ResizablePanelGroup direction="horizontal" className="size-full">
              <ResizablePanel order={1} defaultSize={selectedCellId ? 70 : 100} minSize={50}>
                <ScrollArea tableViewport className="size-full" ref={scrollRootRef}>
                  <SqlEditorCenterBottomPanelQueryResultTable
                    selectedCellId={selectedCellId}
                    onSelectedCellId={setSelectedCellId}
                    columns={columns}
                    rows={rows}
                    isLoading={isLoading}
                  />
                  <ScrollBar orientation="horizontal" />
                </ScrollArea>
              </ResizablePanel>

              {selectedCellId && <SqlEditorResizableHandle />}

              {selectedCellId && (
                <ResizablePanel
                  className="transition-none"
                  order={2}
                  collapsible
                  defaultSize={30}
                  minSize={20}
                  onCollapse={() => setSelectedCellId(undefined)}
                >
                  <SqlEditorCenterBottomPanelSelectedCell
                    selectedCellId={selectedCellId}
                    columns={columns}
                    rows={rows}
                    onClose={() => setSelectedCellId(undefined)}
                  />
                </ResizablePanel>
              )}
            </ResizablePanelGroup>
          </TabsContent>
          <TabsContent value="chart"></TabsContent>
        </Tabs>
      )}
    </>
  );
}
