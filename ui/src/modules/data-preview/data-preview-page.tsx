import { useState } from 'react';

import { useNavigate, useParams } from '@tanstack/react-router';
import { Table } from 'lucide-react';
import { useInView } from 'react-intersection-observer';

import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { TableDataUploadDialog } from '@/modules/shared/table-data-upload-dialog/table-data-upload-dialog';

import { DataPreviewTable } from '../shared/data-preview-table/data-preview-table';
import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
import { DataPreviewPageToolbar } from './data-preview-page-toolbar';
import { useGetInfiniteTablePreviewData } from './use-infinite-preview-data';
import { usePreviewDataSearch } from './use-preview-data-search';

export function DataPreviewPage() {
  const navigate = useNavigate();

  const [search, setSearch] = useState('');
  const [isLoadDataDialogOpened, setIsLoadDataDialogOpened] = useState(false);

  const { databaseName, schemaName, tableName } = useParams({
    from: '/databases/_dataPagesLayout/$databaseName/schemas/$schemaName/tables/$tableName/data-preview/',
  });

  const { ref: loadMoreRef, inView } = useInView({
    threshold: 0.1,
  });

  const {
    data: previewData,
    isLoading: isLoadingPreviewData,
    isFetching: isPreviewDataFetching,
    isFetchingNextPage,
    hasNextPage,
    loadMore,
    refetch: refetchPreviewData,
  } = useGetInfiniteTablePreviewData({
    databaseName,
    schemaName,
    tableName,
  });

  // Load more data when the load more element comes into view
  if (inView && hasNextPage && !isFetchingNextPage) {
    loadMore();
  }

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
      <PageHeader
        title={tableName}
        Icon={Table}
        Action={
          <Button
            size="sm"
            onClick={() => setIsLoadDataDialogOpened(true)}
            disabled={isPreviewDataFetching}
          >
            Load Data
          </Button>
        }
      />
      <Tabs defaultValue="data-preview" variant="underline" className="size-full">
        <TabsList className="px-4">
          <TabsTrigger
            onClick={() =>
              navigate({
                to: '/databases/$databaseName/schemas/$schemaName/tables/$tableName/columns',
                params: {
                  databaseName,
                  schemaName,
                  tableName,
                },
              })
            }
            value="columns"
          >
            Columns
          </TabsTrigger>
          <TabsTrigger value="data-preview">Data Preview</TabsTrigger>
        </TabsList>
        <TabsContent value="data-preview" className="m-0">
          <DataPreviewPageToolbar
            previewData={searchedPreviewData}
            isFetchingPreviewData={isPreviewDataFetching}
            search={search}
            onSearch={setSearch}
            onRefetchPreviewData={refetchPreviewData}
          />
          {isPreviewEmpty && !isLoadingPreviewData ? (
            <PageEmptyContainer
              variant="tabs"
              Icon={Table}
              title="No Data Found"
              description={
                isPreviewEmptyDueToSearch
                  ? 'No data matches your search.'
                  : 'No data has been loaded for this table yet.'
              }
            />
          ) : (
            <PageScrollArea tabs>
              <DataPreviewTable columns={searchedPreviewData} isLoading={isLoadingPreviewData} />
              {hasNextPage && <div ref={loadMoreRef} />}
            </PageScrollArea>
          )}
        </TabsContent>
      </Tabs>
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
