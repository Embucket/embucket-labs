import { useState } from 'react';

import { useGetInfiniteTablePreviewData } from '@/modules/data-preview/use-infinite-preview-data';
import { useGetNavigationTrees } from '@/orval/navigation-trees';
import { useGetTableColumns } from '@/orval/tables';

import { useEditorSettingsStore } from '../../editor-settings-store';
import { EditorLeftPanelTableColumns } from './editor-left-panel-table-columns';
import { EditorLeftPanelTableColumnsPreviewDialog } from './editor-left-panel-table-columns-preview-dialog';
import { EditorLeftPanelTableColumnsToolbar } from './editor-left-panel-table-columns-toolbar';

export function EditorLeftBottomPanel() {
  const selectedTree = useEditorSettingsStore((state) => state.selectedTree);
  const [open, setOpen] = useState(false);

  const { isFetching: isFetchingNavigationTrees } = useGetNavigationTrees();

  const isEnabled =
    !isFetchingNavigationTrees &&
    !!selectedTree?.databaseName &&
    !!selectedTree.schemaName &&
    !!selectedTree.tableName;

  const {
    data: previewData,
    isFetching: isPreviewDataFetching,
    isFetchingNextPage,
    hasNextPage,
    loadMore,
  } = useGetInfiniteTablePreviewData({
    databaseName: selectedTree?.databaseName ?? '',
    schemaName: selectedTree?.schemaName ?? '',
    tableName: selectedTree?.tableName ?? '',
    enabled: isEnabled,
  });

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

  if (!selectedTree?.tableName) {
    return null;
  }

  return (
    <>
      <EditorLeftPanelTableColumnsToolbar
        previewData={previewData}
        selectedTree={selectedTree}
        onSetOpen={setOpen}
      />

      <EditorLeftPanelTableColumns isLoadingColumns={isLoadingColumns} columns={columns ?? []} />

      <EditorLeftPanelTableColumnsPreviewDialog
        previewData={previewData}
        isPreviewDataFetching={isPreviewDataFetching}
        isFetchingNextPage={isFetchingNextPage}
        hasNextPage={hasNextPage}
        loadMore={loadMore}
        selectedTree={selectedTree}
        opened={open}
        onSetOpened={setOpen}
      />
    </>
  );
}
