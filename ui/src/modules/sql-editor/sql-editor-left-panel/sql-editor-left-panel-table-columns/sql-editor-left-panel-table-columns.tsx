import { useState } from 'react';

import { Hash } from 'lucide-react';

import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { useGetTableColumns } from '@/orval/tables';

import type { SelectedTree } from '../sql-editor-left-panel-trees/sql-editor-left-panel-trees-items';
import { SqlEditorLeftPanelTableColumnsPreviewDialog } from './sql-editor-left-panel-table-columns-preview-dialog/sql-editor-left-panel-table-columns-preview-dialog';
import { SqlEditorLeftPanelTableColumnsToolbar } from './sql-editor-left-panel-table-columns-toolbar';

interface ColumnItemProps {
  name: string;
  type: string;
}

function ColumnItem({ name, type }: ColumnItemProps) {
  return (
    <div className="flex items-center justify-between text-xs select-none">
      <div className="flex items-center overflow-hidden py-2">
        <Hash className="text-muted-foreground size-4 flex-shrink-0" />
        <p className="mx-2 truncate">{name}</p>
      </div>
      <span className="text-muted-foreground flex-shrink-0">{type}</span>
    </div>
  );
}

interface TableColumnsProps {
  selectedTree?: SelectedTree;
}

export function SqlEditorLeftPanelTableColumns({ selectedTree }: TableColumnsProps) {
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
      {/* TODO: Hardcode */}
      <ScrollArea className="h-[calc(100%-36px-16px)] py-2">
        <div className="px-4">
          {columns.map((column, index) => (
            <ColumnItem key={index} name={column.name} type={column.type} />
          ))}
        </div>
        <ScrollBar orientation="vertical" />
      </ScrollArea>

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
