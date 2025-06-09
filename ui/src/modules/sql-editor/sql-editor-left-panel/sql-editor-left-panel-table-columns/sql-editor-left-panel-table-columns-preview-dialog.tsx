import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { DataPreviewTable } from '@/modules/shared/data-preview-table/data-preview-table';
import type { SelectedTree } from '@/modules/shared/trees/trees-items';
import type { TablePreviewDataColumn } from '@/orval/models';

interface SqlEditorLeftPanelTableColumnsPreviewDialogProps {
  opened: boolean;
  selectedTree: SelectedTree;
  onSetOpened: (opened: boolean) => void;
  previewData: TablePreviewDataColumn[];
  isPreviewDataFetching: boolean;
}

export function SqlEditorLeftPanelTableColumnsPreviewDialog({
  opened,
  onSetOpened,
  previewData,
  isPreviewDataFetching,
}: SqlEditorLeftPanelTableColumnsPreviewDialogProps) {
  return (
    <Dialog open={opened} onOpenChange={onSetOpened}>
      {/* TODO: Hardcode */}
      <DialogContent className="max-h-[calc(100vh-32px)]! w-fit max-w-[calc(100vw-32px)]! min-w-80">
        <DialogHeader>
          <DialogTitle>Preview Table Data</DialogTitle>
        </DialogHeader>
        {/* TODO: Hardcode */}
        <ScrollArea
          tableViewport
          className="size-full max-h-[calc(100vh-32px-48px-18px-24px)]! max-w-[calc(100vw-32px-48px)]!"
        >
          <DataPreviewTable columns={previewData} isLoading={isPreviewDataFetching} />
          <ScrollBar orientation="horizontal" />
          <ScrollBar orientation="vertical" />
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
}
