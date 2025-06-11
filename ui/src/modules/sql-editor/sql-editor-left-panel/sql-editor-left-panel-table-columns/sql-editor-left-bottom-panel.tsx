import { useState } from 'react';

import { useGetNavigationTrees } from '@/orval/navigation-trees';
import { useGetTableColumns, useGetTablePreviewData } from '@/orval/tables';

import { useSqlEditorSettingsStore } from '../../sql-editor-settings-store';
import { SqlEditorLeftPanelTableColumns } from './sql-editor-left-panel-table-columns';
import { SqlEditorLeftPanelTableColumnsPreviewDialog } from './sql-editor-left-panel-table-columns-preview-dialog';
import { SqlEditorLeftPanelTableColumnsToolbar } from './sql-editor-left-panel-table-columns-toolbar';

export function SqlEditorLeftBottomPanel() {
  const selectedTree = useSqlEditorSettingsStore((state) => state.selectedTree);
  const [open, setOpen] = useState(false);

  const { isFetching: isFetchingNavigationTrees } = useGetNavigationTrees();

  const isEnabled =
    !isFetchingNavigationTrees &&
    !!selectedTree?.databaseName &&
    !!selectedTree.schemaName &&
    !!selectedTree.tableName;

  const { data: { items: previewData } = {}, isFetching: isPreviewDataFetching } =
    useGetTablePreviewData(
      selectedTree?.databaseName ?? '',
      selectedTree?.schemaName ?? '',
      selectedTree?.tableName ?? '',
      undefined,
      {
        query: {
          enabled: isEnabled,
        },
      },
    );

  const { data: { items: columns } = {}, isLoading: isLoadingColumns } = useGetTableColumns(
    selectedTree?.databaseName ?? '',
    selectedTree?.schemaName ?? '',
    selectedTree?.tableName ?? '',
    {
      query: {
        enabled: isEnabled,
      },
    },
  );

  if (!selectedTree) {
    return null;
  }

  return (
    <>
      <SqlEditorLeftPanelTableColumnsToolbar
        previewData={previewData ?? []}
        selectedTree={selectedTree}
        onSetOpen={setOpen}
      />

      <SqlEditorLeftPanelTableColumns isLoadingColumns={isLoadingColumns} columns={columns ?? []} />

      <SqlEditorLeftPanelTableColumnsPreviewDialog
        previewData={previewData ?? []}
        isPreviewDataFetching={isPreviewDataFetching}
        selectedTree={selectedTree}
        opened={open}
        onSetOpened={setOpen}
      />
    </>
  );
}
