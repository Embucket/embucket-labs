import { useState } from 'react';

import { useGetTableColumns } from '@/orval/tables';

import { useSqlEditorSettingsStore } from '../../sql-editor-settings-store';
import { SqlEditorLeftPanelTableColumns } from './sql-editor-left-panel-table-columns';
import { SqlEditorLeftPanelTableColumnsPreviewDialog } from './sql-editor-left-panel-table-columns-preview-dialog/sql-editor-left-panel-table-columns-preview-dialog';
import { SqlEditorLeftPanelTableColumnsToolbar } from './sql-editor-left-panel-table-columns-toolbar';

export function SqlEditorLeftBottomPanel() {
  const selectedTree = useSqlEditorSettingsStore((state) => state.selectedTree);
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
