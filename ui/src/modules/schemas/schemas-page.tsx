import { useState } from 'react';

import { useParams } from '@tanstack/react-router';
import { FolderTree } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { useGetSchemas } from '@/orval/schemas';

import { CreateSchemaDialog } from '../shared/create-schema-dialog/create-schema-dialog';
import { DataPageHeader } from '../shared/data-page/data-page-header';
import { DataPageTrees } from '../shared/data-page/databases-page-trees';
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
            title="Database schemas"
            secondaryText={`${schemas?.length} schemas found`}
            Action={<Button onClick={() => setOpened(true)}>Add Schema</Button>}
          />
          {schemas?.length ? (
            // TODO: Hardcode
            <ScrollArea className="h-[calc(100vh-117px-32px-2px)]">
              <div className="flex size-full flex-col p-4">
                <ScrollArea tableViewport>
                  <SchemasTable schemas={schemas} isLoading={isFetching} />
                  <ScrollBar orientation="horizontal" />
                </ScrollArea>
              </div>
              <ScrollBar orientation="vertical" />
            </ScrollArea>
          ) : (
            <EmptyContainer
              // TODO: Hardcode
              className="h-[calc(100vh-117px-32px-2px)]"
              Icon={FolderTree}
              title="No Schemas Found"
              description="No schemas have been created yet. Create a schema to get started."
            />
          )}
        </ResizablePanel>
      </ResizablePanelGroup>
      <CreateSchemaDialog opened={opened} onSetOpened={setOpened} databaseName={databaseName} />
    </>
  );
}
