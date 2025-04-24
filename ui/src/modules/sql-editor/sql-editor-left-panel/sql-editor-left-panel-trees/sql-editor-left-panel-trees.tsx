import { useState } from 'react';

import { Database } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { SidebarMenu } from '@/components/ui/sidebar';
import type { NavigationTreeDatabase } from '@/orval/models';

import { SqlEditorUploadDialog } from '../../sql-editor-upload-dropzone/sql-editor-upload-dialog';
import {
  SqlEditorLeftPanelTreesDatabases,
  type SelectedTree,
} from './sql-editor-left-panel-trees-items';

interface SqlEditorLeftPanelTreesProps {
  selectedTree?: SelectedTree;
  isFetchingNavigationTrees: boolean;
  navigationTrees: NavigationTreeDatabase[];
  onSetSelectedTree: (tree: SelectedTree) => void;
}

export function SqlEditorLeftPanelTrees({
  selectedTree,
  isFetchingNavigationTrees,
  navigationTrees,
  onSetSelectedTree,
}: SqlEditorLeftPanelTreesProps) {
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const handleSetSelectedTree = (tree: SelectedTree) => {
    onSetSelectedTree(tree);
  };

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
        <SidebarMenu className="block w-full px-4 select-none">
          <SqlEditorLeftPanelTreesDatabases
            databases={navigationTrees}
            selectedTree={selectedTree}
            onSetSelectedTree={handleSetSelectedTree}
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
