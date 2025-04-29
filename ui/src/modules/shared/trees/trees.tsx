import { useState } from 'react';

import { Database } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { SidebarMenu } from '@/components/ui/sidebar';
import { useSqlEditorSettingsStore } from '@/modules/sql-editor/sql-editor-settings-store';
import { SqlEditorUploadDialog } from '@/modules/sql-editor/sql-editor-upload-dropzone/sql-editor-upload-dialog';
import type { NavigationTreeDatabase } from '@/orval/models';

import { TreesDatabases } from './trees-items';

interface TreesProps {
  isFetchingNavigationTrees: boolean;
  navigationTrees: NavigationTreeDatabase[];
}

export function Trees({ isFetchingNavigationTrees, navigationTrees }: TreesProps) {
  const selectedTree = useSqlEditorSettingsStore((state) => state.selectedTree);
  const [isDialogOpen, setIsDialogOpen] = useState(false);

  const handleOpenUploadDialog = () => {
    setIsDialogOpen(true);
  };

  if (!isFetchingNavigationTrees && !navigationTrees.length) {
    return (
      <EmptyContainer
        className="absolute text-center text-wrap"
        Icon={Database}
        title="No Databases Available"
        description="Create a database to start organizing your data."
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        onCtaClick={() => {}}
        ctaText="Create database"
      />
    );
  }

  return (
    <>
      <ScrollArea className="size-full py-2">
        <SidebarMenu className="w-full px-2 select-none">
          <TreesDatabases databases={navigationTrees} onOpenUploadDialog={handleOpenUploadDialog} />
        </SidebarMenu>
        <ScrollBar orientation="vertical" />
      </ScrollArea>

      {selectedTree && (
        <SqlEditorUploadDialog
          opened={isDialogOpen}
          onSetOpened={setIsDialogOpen}
          selectedTree={selectedTree}
        />
      )}
    </>
  );
}
