import type { ColumnDef } from '@tanstack/react-table';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import type { Column, Row } from '@/orval/models';

interface QueryResultDataTableProps {
  isLoading: boolean;
  rows: Row[];
  columns: Column[];
}

export function SqlEditorCenterBottomPanelQueryResultTable({
  isLoading,
  rows,
  columns,
}: QueryResultDataTableProps) {
  const columnHelper = createColumnHelper<unknown[]>();

  const tableColumns: ColumnDef<Row>[] = columns.map((column) =>
    columnHelper.accessor((row) => row[columns.indexOf(column)], {
      header: column.name,
      cell: (info) => String(info.getValue()),
      meta: {
        headerClassName: 'capitalize',
      },
    }),
  );

  return <DataTable removeLRBorders columns={tableColumns} data={rows} isLoading={isLoading} />;
}
