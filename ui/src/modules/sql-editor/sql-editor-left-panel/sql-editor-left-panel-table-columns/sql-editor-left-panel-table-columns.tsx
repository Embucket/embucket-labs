import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import type { TableColumn } from '@/orval/models';

import { SqlEditorLeftPanelTableColumn } from './sql-editor-left-panel-table-column';

interface TableColumnsProps {
  columns?: TableColumn[];
}

export function SqlEditorLeftPanelTableColumns({ columns }: TableColumnsProps) {
  if (!columns?.length) {
    return null;
  }

  return (
    // TODO: Hardcode
    <ScrollArea className="h-[calc(100%-36px-16px)] py-2">
      <div className="px-4">
        {columns.map((column, index) => (
          <SqlEditorLeftPanelTableColumn key={index} name={column.name} type={column.type} />
        ))}
      </div>
      <ScrollBar orientation="vertical" />
    </ScrollArea>
  );
}
