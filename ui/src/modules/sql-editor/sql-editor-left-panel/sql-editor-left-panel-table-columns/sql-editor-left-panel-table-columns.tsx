import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import type { TableColumn } from '@/orval/models';

import { SqlEditorLeftPanelTableColumn } from './sql-editor-left-panel-table-column';
import { SqlEditorLeftPanelTableColumnsSkeleton } from './sql-editor-left-panel-table-columns-skeleton';

interface TableColumnsProps {
  columns: TableColumn[];
  isLoadingColumns: boolean;
}

export function SqlEditorLeftPanelTableColumns({ columns, isLoadingColumns }: TableColumnsProps) {
  return (
    // TODO: Hardcode
    <ScrollArea className="h-[calc(100%-36px-16px)] py-2">
      <div className="px-4">
        {isLoadingColumns || !columns.length ? (
          <SqlEditorLeftPanelTableColumnsSkeleton />
        ) : (
          columns.map((column, index) => (
            <SqlEditorLeftPanelTableColumn key={index} name={column.name} type={column.type} />
          ))
        )}
      </div>
      <ScrollBar orientation="vertical" />
    </ScrollArea>
  );
}
