import type { ColumnDef } from '@tanstack/react-table';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import type { Column } from '@/orval/models';

interface QueryResultDataTableProps {
  isLoading: boolean;
  rows: unknown[][];
  columns: Column[];
}

export function QueryResultDataTable({ isLoading, rows, columns }: QueryResultDataTableProps) {
  const columnHelper = createColumnHelper<unknown[]>();

  const tableColumns: ColumnDef<unknown[], string>[] = columns.map((column) =>
    columnHelper.accessor((row) => row[columns.indexOf(column)], {
      header: column.name,
      cell: (info) => info.getValue(),
      meta: {
        headerClassName: 'capitalize',
      },
    }),
  );

  return <DataTable columns={tableColumns} data={rows} isLoading={isLoading} />;
}
