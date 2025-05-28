import { useState } from 'react';

import { useParams } from '@tanstack/react-router';
import { Database, FolderTree } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { useGetSchemas } from '@/orval/schemas';

import { CreateSchemaDialog } from '../shared/create-schema-dialog/create-schema-dialog';
import { DataPageTrees } from '../shared/data-page/data-page-trees';
import { PageEmptyContainer } from '../shared/page/page-empty-container';
import { PageHeader } from '../shared/page/page-header';
import { PageScrollArea } from '../shared/page/page-scroll-area';
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
          <PageHeader
            title={databaseName}
            Icon={Database}
            Action={
              <Button size="sm" disabled={isFetching} onClick={() => setOpened(true)}>
                Add Schema
              </Button>
            }
          />
          {!schemas?.length ? (
            <PageEmptyContainer
              Icon={FolderTree}
              title="No Schemas Found"
              description="No schemas have been found for this database."
            />
          ) : (
            <>
              <SchemasPageToolbar schemas={schemas} isFetchingSchemas={isFetching} />
              <PageScrollArea>
                <SchemasTable isLoading={isFetching} schemas={schemas} />
              </PageScrollArea>
            </>
          )}
        </ResizablePanel>
      </ResizablePanelGroup>
      <CreateSchemaDialog opened={opened} onSetOpened={setOpened} databaseName={databaseName} />
    </>
  );
}
