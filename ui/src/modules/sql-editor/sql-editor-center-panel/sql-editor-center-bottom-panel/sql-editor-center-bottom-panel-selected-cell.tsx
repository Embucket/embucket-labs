import { X } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ScrollArea } from '@/components/ui/scroll-area';
import type { Column, Row } from '@/orval/models';

function getDisplayValue(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') {
    const trimmed = value.trim();
    const looksLikeJson =
      (trimmed.startsWith('[') && trimmed.endsWith(']')) ||
      (trimmed.startsWith('{') && trimmed.endsWith('}'));
    if (looksLikeJson) {
      try {
        const parsed: unknown = JSON.parse(trimmed);
        return typeof parsed === 'string' ? parsed : JSON.stringify(parsed, null, 2);
      } catch {
        return value;
      }
    }
    return value;
  }
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return '[Unserializable]';
  }
}

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

  const displayValue = getDisplayValue(value);

  return (
    <div className="flex size-full flex-col border-t">
      <div className="flex items-center gap-2 border-b px-3 py-2">
        <div className="flex flex-col">
          <span className="text-sm font-semibold">{column.name}</span>
          <div className="text-muted-foreground mt-px text-xs">
            Row {rowIndex + 1}, Column {colIndex + 1}
          </div>
        </div>
        <div className="ml-auto flex items-center gap-2">
          <Button size="icon" variant="ghost" className="size-6" onClick={onClose}>
            <X className="size-4" />
          </Button>
        </div>
      </div>
      <ScrollArea className="h-[calc(100%-58px-16px)] p-2">
        <pre className="bg-muted rounded-md p-2 text-xs break-words whitespace-pre-wrap">
          {displayValue}
        </pre>
      </ScrollArea>
    </div>
  );
}
