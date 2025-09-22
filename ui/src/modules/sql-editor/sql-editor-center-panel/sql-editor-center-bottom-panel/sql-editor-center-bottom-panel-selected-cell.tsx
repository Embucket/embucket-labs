import { X } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ScrollArea } from '@/components/ui/scroll-area';
import type { Column, Row } from '@/orval/models';

interface SelectedCellDetailsProps {
  selectedCellId: string;
  rows: Row[];
  columns: Column[];
  onClose: () => void;
}

export function SqlEditorCenterBottomPanelSelectedCell({
  selectedCellId,
  rows,
  columns,
  onClose,
}: SelectedCellDetailsProps) {
  const [rowId, colId] = selectedCellId.split(':');
  const rowIndex = Number(rowId);
  const colIndex = Number(colId);

  const column = columns[colIndex];
  const value = rows[rowIndex]?.[colIndex];

  return (
    <div className="flex size-full flex-col border">
      <div className="flex items-center gap-2 border-b px-3 py-2">
        <div className="text-sm font-medium">Cell details</div>
        <div className="ml-auto flex items-center gap-2">
          <Button size="icon" variant="ghost" className="size-7" onClick={onClose}>
            <X className="size-4" />
          </Button>
        </div>
      </div>
      <ScrollArea className="size-full px-3 py-2">
        <div className="text-muted-foreground mb-2 text-xs">
          Row {rowIndex + 1}, Column {colIndex + 1}
          {column.name ? ` • ${column.name}` : ''}
        </div>
        <pre className="bg-muted rounded-md p-3 text-sm break-words whitespace-pre-wrap">
          {String(value)}
        </pre>
      </ScrollArea>
    </div>
  );
}
