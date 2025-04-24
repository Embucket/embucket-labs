import { useState } from 'react';

import { useGetTableColumns } from '@/orval/tables';

import type { SelectedTree } from '../sql-editor-left-panel-trees/sql-editor-left-panel-trees-items';
import { SqlEditorLeftPanelTableColumns } from './sql-editor-left-panel-table-columns';
import { SqlEditorLeftPanelTableColumnsPreviewDialog } from './sql-editor-left-panel-table-columns-preview-dialog/sql-editor-left-panel-table-columns-preview-dialog';
import { SqlEditorLeftPanelTableColumnsToolbar } from './sql-editor-left-panel-table-columns-toolbar';

interface SqlEditorLeftPanelBottomPanelProps {
  selectedTree?: SelectedTree;
}

export function SqlEditorLeftPanelBottomPanel({
  selectedTree,
}: SqlEditorLeftPanelBottomPanelProps) {
  const [open, setOpen] = useState(false);

  const { data: { items: columns } = {} } = useGetTableColumns(
    selectedTree?.databaseName ?? '',
    selectedTree?.schemaName ?? '',
    selectedTree?.tableName ?? '',
    { query: { enabled: !!selectedTree } },
  );

  if (!columns?.length) {
    return null;
  }

  return (
    <>
      <SqlEditorLeftPanelTableColumnsToolbar selectedTree={selectedTree} onSetOpen={setOpen} />
      <SqlEditorLeftPanelTableColumns columns={columns} />
      {selectedTree && (
        <SqlEditorLeftPanelTableColumnsPreviewDialog
          selectedTree={selectedTree}
          opened={open}
          onSetOpened={setOpen}
        />
      )}
    </>
  );
}
