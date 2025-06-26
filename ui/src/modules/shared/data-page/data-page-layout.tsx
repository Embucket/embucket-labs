import { Outlet } from '@tanstack/react-router';

import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';

import { DataPageTrees } from './data-page-trees';

export function DataPagesLayout() {
  return (
    <ResizablePanelGroup direction="horizontal">
      <ResizablePanel collapsible defaultSize={20} minSize={20} order={1}>
        <DataPageTrees />
      </ResizablePanel>
      <ResizableHandle withHandle />
      <ResizablePanel collapsible defaultSize={80} order={2}>
        <Outlet />
      </ResizablePanel>
    </ResizablePanelGroup>
  );
}
