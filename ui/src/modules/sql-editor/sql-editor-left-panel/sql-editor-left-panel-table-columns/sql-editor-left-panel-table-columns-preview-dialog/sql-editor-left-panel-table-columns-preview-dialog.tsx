import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import type { SelectedTree } from '@/modules/shared/trees/trees-items';
import { useGetTablePreviewData } from '@/orval/tables';

import { SqlEditorLeftPanelTableColumnsPreviewDialogTable } from './sql-editor-left-panel-table-columns-preview-dialog-table';

interface SqlEditorLeftPanelTableColumnsPreviewDialogProps {
  opened: boolean;
  selectedTree: SelectedTree;
  onSetOpened: (opened: boolean) => void;
}

export function SqlEditorLeftPanelTableColumnsPreviewDialog({
  opened,
  onSetOpened,
  selectedTree,
}: SqlEditorLeftPanelTableColumnsPreviewDialogProps) {
  const { data: { items: columns } = {}, isFetching } = useGetTablePreviewData(
    selectedTree.databaseName,
    selectedTree.schemaName,
    selectedTree.tableName,
  );

  if (!columns) {
    return null;
  }

  return (
    <Dialog open={opened} onOpenChange={onSetOpened}>
      {/* TODO: Hardcode */}
      <DialogContent className="max-h-[calc(100vh-32px)]! w-fit max-w-[calc(100vw-32px)]!">
        <DialogHeader>
          <DialogTitle>Preview Table Data</DialogTitle>
        </DialogHeader>
        {/* TODO: Hardcode */}
        <ScrollArea
          tableViewport
          className="size-full max-h-[calc(100vh-32px-48px-18px-24px)]! max-w-[calc(100vw-32px-48px)]!"
        >
          <SqlEditorLeftPanelTableColumnsPreviewDialogTable
            columns={columns}
            isLoading={isFetching}
          />
          <ScrollBar orientation="horizontal" />
          <ScrollBar orientation="vertical" />
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
}
