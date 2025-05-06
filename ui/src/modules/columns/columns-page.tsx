import { useState } from 'react';

import { useParams } from '@tanstack/react-router';
import { Columns } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { TableDataUploadDialog } from '@/modules/shared/table-data-upload-dialog/table-data-upload-dialog';

import { DataPageHeader } from '../shared/data-page/data-page-header';
import { DataPageTrees } from '../shared/data-page/databases-page-trees';

export function ColumnsPage() {
  const [isLoadDataDialogOpened, setIsLoadDataDialogOpened] = useState(false);
  const { databaseName, schemaName, tableName } = useParams({
    from: '/databases/$databaseName/schemas/$schemaName/tables/$tableName/columns/',
  });

  return (
    <>
      <ResizablePanelGroup direction="horizontal">
        <ResizablePanel collapsible defaultSize={20} minSize={20} order={1}>
          <DataPageTrees />
        </ResizablePanel>
        <ResizableHandle withHandle />
        <ResizablePanel collapsible defaultSize={20} order={1}>
          <DataPageHeader
            title="Table columns"
            secondaryText="0 columns found"
            Action={
              <Button onClick={() => setIsLoadDataDialogOpened(true)} disabled={!tableName}>
                Load Data
              </Button>
            }
          />
          <EmptyContainer
            // TODO: Hardcode
            className="h-[calc(100vh-117px-32px-2px)]"
            Icon={Columns}
            title="No Columns Found"
            description="No columns have been created yet. Create a column to get started."
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
