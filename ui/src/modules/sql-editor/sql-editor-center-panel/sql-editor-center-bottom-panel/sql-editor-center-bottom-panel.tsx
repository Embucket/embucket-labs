import { ArrowDownToLine, Search, TextSearch } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { Button } from '@/components/ui/button';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs2';
import type { QueryRecord } from '@/orval/models';

import { SqlEditorCenterBottomPanelQueryResultTable } from './sql-editor-center-bottom-panel-query-result-table';

interface SqlEditorCenterPanelQueryColumnsProps {
  isLoading: boolean;
  isIdle: boolean;
  queryRecord?: QueryRecord;
}

export function SqlEditorCenterBottomPanel({
  isLoading,
  isIdle,
  queryRecord,
}: SqlEditorCenterPanelQueryColumnsProps) {
  const columns = queryRecord?.result.columns ?? [];
  const rows = queryRecord?.result.rows ?? [];
  const noFields = !columns.length && !isLoading;

  const rowCount = rows.length.toString();
  const columnCount = columns.length.toString();
  const executionTime = queryRecord ? queryRecord.durationMs / 1000 : 0; // Convert ms to seconds

  return (
    <>
      {isIdle && !queryRecord && (
        <EmptyContainer
          Icon={TextSearch}
          title="No Results Yet"
          description="Once you run a query, results will be displayed here."
        />
      )}

      {!noFields && (
        <Tabs defaultValue="results" className="size-full">
          <TabsList className="px-4">
            <TabsTrigger value="results">Results</TabsTrigger>
            <TabsTrigger disabled value="chart">
              Chart
            </TabsTrigger>
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
          <TabsContent value="results" className="m-0 size-full">
            <ScrollArea tableViewport className="size-full">
              <SqlEditorCenterBottomPanelQueryResultTable
                columns={columns}
                rows={rows}
                isLoading={isLoading}
              />
              <ScrollBar orientation="horizontal" />
            </ScrollArea>
          </TabsContent>
          <TabsContent value="chart"></TabsContent>
        </Tabs>
      )}
    </>
  );
}
