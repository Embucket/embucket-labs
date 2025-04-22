import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { useGetTablePreviewData } from '@/orval/tables';

import type { SelectedTree } from '../sql-editor-left-panel/sql-editor-left-panel-databases';
import { SqlEditorPreviewDialogTable } from './sql-editor-preview-dialog-table';

interface SqlEditorPreviewDialogProps {
  opened: boolean;
  selectedTree: SelectedTree;
  onSetOpened: (opened: boolean) => void;
}

export function SqlEditorPreviewDialog({
  opened,
  onSetOpened,
  selectedTree,
}: SqlEditorPreviewDialogProps) {
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
      <DialogContent className="sm:max-w-4xl">
        <DialogHeader>
          <DialogTitle>Preview Table Data</DialogTitle>
        </DialogHeader>
        <SqlEditorPreviewDialogTable columns={columns} isLoading={isFetching} />
      </DialogContent>
    </Dialog>
  );
}
