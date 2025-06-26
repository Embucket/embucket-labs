import { useState } from 'react';

import { useNavigate, useParams } from '@tanstack/react-router';
import { Columns, Table } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { TableDataUploadDialog } from '@/modules/shared/table-data-upload-dialog/table-data-upload-dialog';
import { useGetTableColumns } from '@/orval/tables';

import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
import { ColumnsTable } from './columns-page-table';
import { ColumnsPageToolbar } from './columns-page-toolbar';
import { useColumnsSearch } from './use-columns-search';

export function ColumnsPage() {
  const navigate = useNavigate();
  const [search, setSearch] = useState('');

  const [isLoadDataDialogOpened, setIsLoadDataDialogOpened] = useState(false);
  const { databaseName, schemaName, tableName } = useParams({
    from: '/databases/_dataPagesLayout/$databaseName/schemas/$schemaName/tables/$tableName/columns/',
  });

  const {
    data: { items: columns } = {},
    isFetching: isFetchingColumns,
    isLoading: isLoadingColumns,
    refetch: refetchColumns,
  } = useGetTableColumns(databaseName, schemaName, tableName);

  const {
    searchedColumns,
    isEmpty: isColumnsEmpty,
    isEmptyDueToSearch: isColumnsEmptyDueToSearch,
  } = useColumnsSearch({
    columns,
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
            disabled={isFetchingColumns}
          >
            Load Data
          </Button>
        }
      />
      <Tabs defaultValue="columns" variant="underline" className="size-full">
        <TabsList className="px-4">
          <TabsTrigger value="columns">Columns</TabsTrigger>
          <TabsTrigger
            onClick={() =>
              navigate({
                to: '/databases/$databaseName/schemas/$schemaName/tables/$tableName/data-preview',
                params: {
                  databaseName,
                  schemaName,
                  tableName,
                },
              })
            }
            value="data-preview"
          >
            Data Preview
          </TabsTrigger>
        </TabsList>
        <TabsContent value="columns" className="m-0">
          <ColumnsPageToolbar
            columns={searchedColumns}
            isFetchingColumns={isFetchingColumns}
            search={search}
            onSearch={setSearch}
            onRefetchColumns={refetchColumns}
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
