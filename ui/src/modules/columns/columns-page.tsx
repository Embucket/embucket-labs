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
import { ColumnsPagePreviewDataToolbar } from './columns-page-preview-data-toolbar';
import { ColumnsTable } from './columns-page-table';
import { ColumnsPageToolbar } from './columns-page-toolbar';
import { useColumnsSearch } from './use-columns-search';
import { usePreviewDataSearch } from './use-preview-data-search';

export function ColumnsPage() {
  const [search, setSearch] = useState('');

  const [isLoadDataDialogOpened, setIsLoadDataDialogOpened] = useState(false);
  const { databaseName, schemaName, tableName } = useParams({
    from: '/databases/$databaseName/schemas/$schemaName/tables/$tableName/columns/',
  });
  const {
    data: { items: columns } = {},
    isFetching: isFetchingColumns,
    isLoading: isLoadingColumns,
  } = useGetTableColumns(databaseName, schemaName, tableName);

  const {
    data: { items: previewData } = {},
    isFetching: isPreviewDataFetching,
    isLoading: isLoadingPreviewData,
  } = useGetTablePreviewData(databaseName, schemaName, tableName);

  const {
    searchedColumns,
    isEmpty: isColumnsEmpty,
    isEmptyDueToSearch: isColumnsEmptyDueToSearch,
  } = useColumnsSearch({
    columns,
    search,
  });

  const {
    searchedPreviewData,
    isEmpty: isPreviewEmpty,
    isEmptyDueToSearch: isPreviewEmptyDueToSearch,
  } = usePreviewDataSearch({
    previewData,
    search,
  });

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
                disabled={isFetchingColumns}
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
              <ColumnsPageToolbar
                columns={searchedColumns}
                isFetchingColumns={isFetchingColumns}
                search={search}
                onSearch={setSearch}
              />
              {isColumnsEmpty && !isLoadingColumns ? (
                <PageEmptyContainer
                  variant="tabs"
                  Icon={Columns}
                  title="No Columns Found"
                  description={
                    isColumnsEmptyDueToSearch
                      ? 'No columns match your search.'
                      : 'No columns have been found for this table.'
                  }
                />
              ) : (
                <PageScrollArea tabs>
                  <ColumnsTable isLoading={isLoadingColumns} columns={searchedColumns} />
                </PageScrollArea>
              )}
            </TabsContent>
            <TabsContent value="data-preview" className="m-0">
              <ColumnsPagePreviewDataToolbar
                previewData={searchedPreviewData}
                isFetchingPreviewData={isPreviewDataFetching}
                search={search}
                onSearch={setSearch}
              />
              {isPreviewEmpty && !isLoadingPreviewData ? (
                <PageEmptyContainer
                  variant="tabs"
                  Icon={Columns}
                  title="No Data Found"
                  description={
                    isPreviewEmptyDueToSearch
                      ? 'No data matches your search.'
                      : 'No data has been loaded for this table yet.'
                  }
                />
              ) : (
                <PageScrollArea tabs>
                  <DataPreviewTable
                    columns={searchedPreviewData}
                    isLoading={isLoadingPreviewData}
                  />
                </PageScrollArea>
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
