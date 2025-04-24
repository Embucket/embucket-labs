import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { useGetTablePreviewData } from '@/orval/tables';

import type { SelectedTree } from '../../sql-editor-left-panel-trees/sql-editor-left-panel-trees-items';
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
      <DialogContent className="sm:max-w-4xl">
        <DialogHeader>
          <DialogTitle>Preview Table Data</DialogTitle>
        </DialogHeader>
        <SqlEditorLeftPanelTableColumnsPreviewDialogTable
          columns={columns}
          isLoading={isFetching}
        />
      </DialogContent>
    </Dialog>
  );
}
