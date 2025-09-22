import type { Cell, ColumnDef } from '@tanstack/react-table';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import type { Column, Row } from '@/orval/models';

import { makeCellId } from '../../sql-editor-utils';

interface QueryResultDataTableProps {
  selectedCellId?: string;
  onSelectedCellId: (cellId: string) => void;
  isLoading: boolean;
  rows: Row[];
  columns: Column[];
}

export function SqlEditorCenterBottomPanelQueryResultTable({
  isLoading,
  selectedCellId,
  onSelectedCellId,
  rows,
  columns,
}: QueryResultDataTableProps) {
  const columnHelper = createColumnHelper<unknown[]>();

  const handleCellClick = (cell: Cell<Row, unknown>) => {
    onSelectedCellId(makeCellId(cell.row.id, cell.column.id));
  };

  const tableColumns: ColumnDef<Row>[] = columns.map((column, colIndex) =>
    columnHelper.accessor((row) => row[colIndex], {
      id: String(colIndex),
      header: column.name,
      cell: (info) => String(info.getValue()),
      meta: {
        headerClassName: 'capitalize',
      },
    }),
  );

  return (
    <DataTable
      onCellClick={handleCellClick}
      selectedCellId={selectedCellId}
      removeLRBorders
      columns={tableColumns}
      data={rows}
      isLoading={isLoading}
    />
  );
}
