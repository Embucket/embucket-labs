// import { Link } from '@tanstack/react-router';
import { useState } from 'react';

import { Database, Search } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { Button } from '@/components/ui/button';
import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { useGetDatabases } from '@/orval/databases';

import { CreateDatabaseDialog } from '../shared/create-database-dialog/create-database-dialog';
import { DataPageTrees } from '../shared/data-page/databases-page-trees';
// import { PageHeader } from '../shared/page/page-header';
import { DatabasesTable } from './databases-page-table';

export function DatabasesPage() {
  const [opened, setOpened] = useState(false);
  const { data: { items: databases } = {}, isFetching } = useGetDatabases();

  return (
    <>
      <ResizablePanelGroup direction="horizontal">
        <ResizablePanel collapsible defaultSize={20} minSize={20} order={1}>
          <DataPageTrees />
        </ResizablePanel>
        <ResizableHandle withHandle />
        <ResizablePanel collapsible defaultSize={20} order={1}>
          <div className="border-b p-4">
            <div className="mb-4 flex items-center justify-between">
              <h1 className="text-lg">Databases</h1>
              <Button onClick={() => setOpened(true)}>Add Database</Button>
            </div>
            <div className="flex items-center justify-between gap-4">
              <p className="text-muted-foreground text-sm text-nowrap">
                {databases?.length} databases found
              </p>
              <InputRoot>
                <InputIcon>
                  <Search />
                </InputIcon>
                <Input className="min-w-80" disabled placeholder="Search" />
              </InputRoot>
            </div>
          </div>
          {databases?.length ? (
            // TODO: Hardcode
            <ScrollArea className="h-[calc(100vh-117px-32px-2px)]">
              <div className="flex size-full flex-col p-4">
                <ScrollArea tableViewport>
                  <DatabasesTable databases={databases} isLoading={isFetching} />
                  <ScrollBar orientation="horizontal" />
                </ScrollArea>
              </div>
              <ScrollBar orientation="vertical" />
            </ScrollArea>
          ) : (
            <EmptyContainer
              // TODO: Hardcode
              className="h-[calc(100vh-117px-32px-2px)]"
              Icon={Database}
              title="No Databases Found"
              description="No databases have been created yet. Create a database to get started."
            />
          )}
        </ResizablePanel>
      </ResizablePanelGroup>
      <CreateDatabaseDialog opened={opened} onSetOpened={setOpened} />
    </>
  );
}
