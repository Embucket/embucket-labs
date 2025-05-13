import { useState } from 'react';

import { useParams } from '@tanstack/react-router';
import { Database, FolderTree } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { useGetSchemas } from '@/orval/schemas';

import { CreateSchemaDialog } from '../shared/create-schema-dialog/create-schema-dialog';
import { DataPageContent } from '../shared/data-page/data-page-content';
import { DataPageHeader } from '../shared/data-page/data-page-header';
import { DataPageTrees } from '../shared/data-page/data-page-trees';
import { SchemasTable } from './schemas-page-table';

export function SchemasPage() {
  const [opened, setOpened] = useState(false);
  const { databaseName } = useParams({ from: '/databases/$databaseName/schemas/' });
  const { data: { items: schemas } = {}, isFetching } = useGetSchemas(databaseName);

  return (
    <>
      <ResizablePanelGroup direction="horizontal">
        <ResizablePanel collapsible defaultSize={20} minSize={20} order={1}>
          <DataPageTrees />
        </ResizablePanel>
        <ResizableHandle withHandle />
        <ResizablePanel collapsible defaultSize={20} order={1}>
          <DataPageHeader
            title={databaseName}
            Icon={Database}
            secondaryText={`${schemas?.length} schemas found`}
            Action={
              <Button disabled={isFetching} onClick={() => setOpened(true)}>
                Add Schema
              </Button>
            }
          />
          <DataPageContent
            isEmpty={!schemas?.length}
            Table={<SchemasTable isLoading={isFetching} schemas={schemas ?? []} />}
            emptyStateIcon={FolderTree}
            emptyStateTitle="No Schemas Found"
            emptyStateDescription="No schemas have been created yet. Create a schema to get started."
          />
        </ResizablePanel>
      </ResizablePanelGroup>
      <CreateSchemaDialog opened={opened} onSetOpened={setOpened} databaseName={databaseName} />
    </>
  );
}
