import { Link } from '@tanstack/react-router';
import { Database } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { useGetDatabases } from '@/orval/databases';

import { PageContent } from '../shared/page/page-content';
import { PageHeader } from '../shared/page/page-header';
import { DatabasesTable } from './databases-page-table';
import { DatabasesPageTrees } from './databases-page-trees';

export function DatabasesPage() {
  const { data: { items: databases } = {}, isFetching } = useGetDatabases();

  return (
    <>
      <PageHeader title="Databases">
        <Link
          className="text-blue-500"
          to="/databases/$databaseId/schemas"
          params={{ databaseId: '1' }}
        >
          Schemas →
        </Link>
      </PageHeader>
      <PageContent>
        <ResizablePanelGroup direction="horizontal" className="min-h-[calc(100vh-65px-32px-32px)]">
          <ResizablePanel collapsible defaultSize={20} minSize={20} order={1}>
            <DatabasesPageTrees />
          </ResizablePanel>
          <ResizableHandle withHandle />
          <ResizablePanel collapsible defaultSize={20} order={1}>
            <div className="flex size-full flex-col">
              {databases?.length ? (
                <ScrollArea tableViewport>
                  <DatabasesTable databases={databases} isLoading={isFetching} />
                  <ScrollBar orientation="horizontal" />
                </ScrollArea>
              ) : (
                <EmptyContainer
                  // TODO: Hardcode
                  className="min-h-[calc(100vh-32px-65px-32px)]"
                  Icon={Database}
                  title="No Databases Found"
                  description="No databases have been created yet. Create a database to get started."
                />
              )}
            </div>
          </ResizablePanel>
        </ResizablePanelGroup>
      </PageContent>
    </>
  );
}
