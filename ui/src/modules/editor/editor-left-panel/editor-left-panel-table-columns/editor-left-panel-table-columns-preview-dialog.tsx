import { useInView } from 'react-intersection-observer';

import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { DataPreviewTable } from '@/modules/shared/data-preview-table/data-preview-table';
import type { SelectedTree } from '@/modules/shared/trees/trees-items';
import type { TablePreviewDataColumn } from '@/orval/models';

interface EditorLeftPanelTableColumnsPreviewDialogProps {
  opened: boolean;
  selectedTree: SelectedTree;
  onSetOpened: (opened: boolean) => void;
  previewData: TablePreviewDataColumn[];
  isPreviewDataFetching: boolean;
  isFetchingNextPage?: boolean;
  hasNextPage?: boolean;
  loadMore?: () => void;
}

export function EditorLeftPanelTableColumnsPreviewDialog({
  opened,
  onSetOpened,
  previewData,
  isPreviewDataFetching,
  isFetchingNextPage,
  hasNextPage,
  loadMore,
}: EditorLeftPanelTableColumnsPreviewDialogProps) {
  const { ref: loadMoreRef, inView } = useInView({
    threshold: 0.1,
  });

  // Load more data when the load more element comes into view
  if (inView && hasNextPage && !isFetchingNextPage && loadMore) {
    loadMore();
  }

  return (
    <Dialog open={opened} onOpenChange={onSetOpened}>
      {/* TODO: Hardcode */}
      <DialogContent className="max-h-[calc(100vh-var(--content-mb)-var(--content-mt))]! w-fit max-w-[calc(100vw-32px)]! min-w-80">
        <DialogHeader>
          <DialogTitle>Preview Table Data</DialogTitle>
        </DialogHeader>
        {/* TODO: Hardcode */}
        <ScrollArea
          tableViewport
          className="size-full max-h-[calc(100vh-var(--content-mb)-var(--content-mt)-48px-18px-24px)]! max-w-[calc(100vw-32px-48px)]!"
        >
          <DataPreviewTable columns={previewData} isLoading={isPreviewDataFetching} />
          {hasNextPage && <div ref={loadMoreRef} />}
          <ScrollBar orientation="horizontal" />
          <ScrollBar orientation="vertical" />
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
}
