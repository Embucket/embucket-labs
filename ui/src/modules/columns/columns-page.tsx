import { useState } from 'react';

import { useParams } from '@tanstack/react-router';
import { Columns, Table } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { TableDataUploadDialog } from '@/modules/shared/table-data-upload-dialog/table-data-upload-dialog';
import { useGetTableColumns } from '@/orval/tables';

import { DataPageContent } from '../shared/data-page/data-page-content';
import { DataPageHeader } from '../shared/data-page/data-page-header';
import { DataPageTrees } from '../shared/data-page/data-page-trees';
import { ColumnsTable } from './columns-page-table';

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

  return (
    <>
      <ResizablePanelGroup direction="horizontal">
        <ResizablePanel collapsible defaultSize={20} minSize={20} order={1}>
          <DataPageTrees />
        </ResizablePanel>
        <ResizableHandle withHandle />
        <ResizablePanel collapsible defaultSize={20} order={1}>
          <DataPageHeader
            title={tableName}
            Icon={Table}
            secondaryText={`${columns?.length} columns found`}
            Action={
              <Button onClick={() => setIsLoadDataDialogOpened(true)} disabled={isFetching}>
                Load Data
              </Button>
            }
          />
          <DataPageContent
            isEmpty={!columns?.length}
            Table={<ColumnsTable isLoading={isFetching} columns={columns ?? []} />}
            emptyStateIcon={Columns}
            emptyStateTitle="No Columns Found"
            emptyStateDescription="No columns have been created yet. Create a column to get started."
          />
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
