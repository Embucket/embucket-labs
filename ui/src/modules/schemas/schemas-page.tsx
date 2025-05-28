import { useState } from 'react';

import { useParams } from '@tanstack/react-router';
import { Database, FolderTree } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { useGetSchemas } from '@/orval/schemas';

import { CreateSchemaDialog } from '../shared/create-schema-dialog/create-schema-dialog';
import { DataPageEmptyContainer } from '../shared/data-page/data-page-empty-container';
import { DataPageHeader } from '../shared/data-page/data-page-header';
import { DataPageScrollArea } from '../shared/data-page/data-page-scroll-area';
import { DataPageTrees } from '../shared/data-page/data-page-trees';
import { SchemasTable } from './schemas-page-table';
import { SchemasPageToolbar } from './schemas-page-toolbar';

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
            Action={
              <Button disabled={isFetching} onClick={() => setOpened(true)}>
                Add Schema
              </Button>
            }
          />
          {!schemas?.length ? (
            <DataPageEmptyContainer
              Icon={FolderTree}
              title="No Schemas Found"
              description="No schemas have been found for this database."
            />
          ) : (
            <>
              <SchemasPageToolbar schemas={schemas} isFetchingSchemas={isFetching} />
              <DataPageScrollArea>
                <SchemasTable isLoading={isFetching} schemas={schemas} />
              </DataPageScrollArea>
            </>
          )}
        </ResizablePanel>
      </ResizablePanelGroup>
      <CreateSchemaDialog opened={opened} onSetOpened={setOpened} databaseName={databaseName} />
    </>
  );
}
