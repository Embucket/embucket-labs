import { useState } from 'react';

import { Database } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { SidebarMenu } from '@/components/ui/sidebar';
import type { NavigationTreeDatabase } from '@/orval/models';

import { useSqlEditorSettingsStore } from '../../sql-editor-settings-store';
import { SqlEditorUploadDialog } from '../../sql-editor-upload-dropzone/sql-editor-upload-dialog';
import { SqlEditorLeftPanelTreesDatabases } from './sql-editor-left-panel-trees-items';

interface SqlEditorLeftPanelTreesProps {
  isFetchingNavigationTrees: boolean;
  navigationTrees: NavigationTreeDatabase[];
}

export function SqlEditorLeftPanelTrees({
  isFetchingNavigationTrees,
  navigationTrees,
}: SqlEditorLeftPanelTreesProps) {
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
          <SqlEditorLeftPanelTreesDatabases
            databases={navigationTrees}
            onOpenUploadDialog={handleOpenUploadDialog}
          />
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
