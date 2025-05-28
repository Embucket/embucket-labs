import { useState } from 'react';

import { useParams } from '@tanstack/react-router';
import { Columns, Table } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs2';
import { TableDataUploadDialog } from '@/modules/shared/table-data-upload-dialog/table-data-upload-dialog';
import { useGetTableColumns, useGetTablePreviewData } from '@/orval/tables';

import { DataPageTrees } from '../shared/data-page/data-page-trees';
import { DataPreviewTable } from '../shared/data-preview-table/data-preview-table';
import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
import { ColumnsPagePreviewDataToolbar } from './columns-page-preview-data-tooblar';
import { ColumnsTable } from './columns-page-table';
import { ColumnsPageToolbar } from './columns-page-tooblar';

export function ColumnsPage() {
  const [isLoadDataDialogOpened, setIsLoadDataDialogOpened] = useState(false);
  const { databaseName, schemaName, tableName } = useParams({
    from: '/databases/$databaseName/schemas/$schemaName/tables/$tableName/columns/',
  });
  const { data: { items: columns } = {}, isFetching } = useGetTableColumns(
    databaseName,
    schemaName,
    tableName,
  );

  const { data: { items: previewData } = {}, isFetching: isPreviewDataFetching } =
    useGetTablePreviewData(databaseName, schemaName, tableName);

  return (
    <>
      <ResizablePanelGroup direction="horizontal">
        <ResizablePanel collapsible defaultSize={20} minSize={20} order={1}>
          <DataPageTrees />
        </ResizablePanel>
        <ResizableHandle withHandle />
        <ResizablePanel collapsible defaultSize={20} order={1}>
          <PageHeader
            title={tableName}
            Icon={Table}
            Action={
              <Button
                size="sm"
                onClick={() => setIsLoadDataDialogOpened(true)}
                disabled={isFetching}
              >
                Load Data
              </Button>
            }
          />
          <Tabs defaultValue="columns" className="size-full">
            <TabsList className="px-4">
              <TabsTrigger value="columns">Columns</TabsTrigger>
              <TabsTrigger value="data-preview">Data Preview</TabsTrigger>
            </TabsList>
            <TabsContent value="columns" className="m-0">
              {!columns?.length ? (
                <PageEmptyContainer
                  tabs
                  Icon={Columns}
                  title="No Columns Found"
                  description="No columns have been found for this table."
                />
              ) : (
                <>
                  <ColumnsPageToolbar columns={columns} isFetchingColumns={isFetching} />
                  <PageScrollArea tabs>
                    <ColumnsTable isLoading={isFetching} columns={columns} />
                  </PageScrollArea>
                </>
              )}
            </TabsContent>
            <TabsContent value="data-preview" className="m-0">
              {!previewData?.length ? (
                <PageEmptyContainer
                  tabs
                  Icon={Columns}
                  title="No Data Found"
                  description="No data has been loaded for this table yet."
                />
              ) : (
                <>
                  <ColumnsPagePreviewDataToolbar
                    previewData={previewData}
                    isFetchingPreviewData={isPreviewDataFetching}
                  />
                  <PageScrollArea tabs>
                    <DataPreviewTable columns={previewData} isLoading={isPreviewDataFetching} />
                  </PageScrollArea>
                </>
              )}
            </TabsContent>
          </Tabs>
        </ResizablePanel>
      </ResizablePanelGroup>
      {databaseName && schemaName && tableName && (
        <TableDataUploadDialog
          opened={isLoadDataDialogOpened}
          onSetOpened={setIsLoadDataDialogOpened}
          databaseName={databaseName}
          schemaName={schemaName}
          tableName={tableName}
        />
      )}
    </>
  );
}
