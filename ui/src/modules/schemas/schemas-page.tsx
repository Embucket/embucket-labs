import { FolderTree, Search } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { Button } from '@/components/ui/button';
import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';

import { DataPageTrees } from '../shared/data-page/databases-page-trees';

export function SchemasPage() {
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
              <h1 className="text-lg">Database schemas</h1>
              <Button>Add Schema</Button>
            </div>
            <div className="flex items-center justify-between gap-4">
              <p className="text-muted-foreground text-sm text-nowrap">0 schemas found</p>
              <InputRoot>
                <InputIcon>
                  <Search />
                </InputIcon>
                <Input className="min-w-80" disabled placeholder="Search" />
              </InputRoot>
            </div>
          </div>
          <EmptyContainer
            // TODO: Hardcode
            className="min-h-[calc(100vh-32px-65px-32px)]"
            Icon={FolderTree}
            title="No Schemas Found"
            description="No schemas have been created yet. Create a schema to get started."
          />
        </ResizablePanel>
      </ResizablePanelGroup>
    </>
  );
}
