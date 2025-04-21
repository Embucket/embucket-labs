import { useState } from 'react';

import { Eye, Hash, Table, X } from 'lucide-react';

import { Button } from '@/components/ui/button';
// import {
//   DropdownMenu,
//   DropdownMenuContent,
//   DropdownMenuItem,
//   DropdownMenuTrigger,
// } from '@/components/ui/dropdown-menu';
import type { TableColumn } from '@/orval/models';
import { useGetTableColumns } from '@/orval/tables';

import { useSqlEditorPanelsState } from '../sql-editor-panels-state-provider';
import { SqlEditorPreviewDialog } from '../sql-editor-preview-dialog/sql-editor-preview-dialog';
import type { SelectedTree } from './sql-editor-left-panel-databases';

const DATA: TableColumn[] = [
  { name: 'ID', type: 'VARCHAR(255)', nullable: 'true', default: 'NULL', description: '' },
  { name: 'EMAIL', type: 'VARCHAR(255)', nullable: 'true', default: 'NULL', description: '' },
  { name: 'AMOUNT', type: 'NUMBER(38, 10)', nullable: 'true', default: 'NULL', description: '' },
  { name: 'DATE', type: 'DATE', nullable: 'true', default: 'NULL', description: '' },
];

interface TableFieldsProps {
  selectedTree?: SelectedTree;
}

export function SqlEditorLeftPanelTableFields({ selectedTree }: TableFieldsProps) {
  const [open, setOpen] = useState(false);

  const { toggleLeftBottomPanel } = useSqlEditorPanelsState();

  const { data: { items: columns = DATA } = {} } = useGetTableColumns(
    selectedTree?.databaseName ?? '',
    selectedTree?.schemaName ?? '',
    selectedTree?.tableName ?? '',
    { query: { enabled: !!selectedTree } },
  );

  if (!columns.length) {
    return null;
  }

  return (
    <div className="w-full px-4 py-2">
      <div className="mb-2 flex items-center justify-between">
        <div className="flex items-center gap-2 py-2 select-none">
          <Table className="size-4" />
          <span className="max-w-32 truncate">{selectedTree?.tableName}</span>
        </div>
        <div className="flex items-center gap-px">
          {/* <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button size="icon" variant="ghost" className="text-muted-foreground size-8">
                <MoreVertical />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent side="right" align="start">
              <DropdownMenuItem onClick={() => {}}>
                <span>Load data</span>
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => {}}>
                <span>Preview data</span>
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu> */}
          <Button
            onClick={() => setOpen(true)}
            size="icon"
            variant="ghost"
            className="text-muted-foreground size-8"
          >
            <Eye />
          </Button>
          <Button
            onClick={toggleLeftBottomPanel}
            size="icon"
            variant="ghost"
            className="text-muted-foreground size-8"
          >
            <X />
          </Button>
        </div>
      </div>

      <div>
        {columns.map((column, index) => (
          <div key={index} className="flex items-center justify-between text-xs select-none">
            <div className="flex items-center py-2">
              <Hash className="text-foreground size-4" />
              <span className="ml-2 max-w-32 truncate">{column.name}</span>
            </div>
            <span className="text-muted-foreground">{column.type}</span>
          </div>
        ))}
      </div>

      {selectedTree && (
        <SqlEditorPreviewDialog selectedTree={selectedTree} opened={open} onSetOpened={setOpen} />
      )}
    </div>
  );
}
