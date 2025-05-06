import { Table } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';

import { DataPageHeader } from '../shared/data-page/data-page-header';
import { DataPageTrees } from '../shared/data-page/databases-page-trees';

export function TablesPage() {
  return (
    <>
      <ResizablePanelGroup direction="horizontal">
        <ResizablePanel collapsible defaultSize={20} minSize={20} order={1}>
          <DataPageTrees />
        </ResizablePanel>
        <ResizableHandle withHandle />
        <ResizablePanel collapsible defaultSize={20} order={1}>
          <DataPageHeader
            title="Schema tables"
            secondaryText="0 tables found"
            Action={<Button>Add Table</Button>}
          />
          <EmptyContainer
            // TODO: Hardcode
            className="h-[calc(100vh-117px-32px-2px)]"
            Icon={Table}
            title="No Tables Found"
            description="No tables have been created yet. Create a table to get started."
          />
        </ResizablePanel>
      </ResizablePanelGroup>
    </>
  );
}
