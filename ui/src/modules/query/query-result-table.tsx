import type { ColumnDef } from '@tanstack/react-table';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import type { Column, Row } from '@/orval/models';

interface QueryResultsTableProps {
  isLoading: boolean;
  rows: Row[];
  columns: Column[];
}

export function QueryResultsTable({ isLoading, rows, columns }: QueryResultsTableProps) {
  const columnHelper = createColumnHelper<unknown[]>();

  const tableColumns: ColumnDef<Row>[] = columns.map((column) =>
    columnHelper.accessor((row) => row[columns.indexOf(column)], {
      header: column.name,
      cell: (info) => String(info.getValue()),
      meta: {
        headerClassName: 'capitalize',
        cellClassName: 'max-w-md truncate',
      },
    }),
  );

  return <DataTable rounded columns={tableColumns} data={rows} isLoading={isLoading} />;
}
