import type { ColumnDef } from '@tanstack/react-table';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import type { TablePreviewDataColumn } from '@/orval/models';

import {
  generateDataPreviewTableRows,
  groupDataPreviewTableColumnsByName,
} from './data-preview-table-utils';

interface DataPreviewTableProps {
  isLoading: boolean;
  columns: TablePreviewDataColumn[];
}

// TODO: Type
export function DataPreviewTable({ isLoading, columns }: DataPreviewTableProps) {
  const columnHelper = createColumnHelper<Record<string, string>>();

  const groupedColumns = groupDataPreviewTableColumnsByName(columns);

  const tableColumns: ColumnDef<Record<string, string>, string>[] = groupedColumns.map((column) =>
    columnHelper.accessor((row) => row[column.name], {
      header: column.name,
      cell: (info) => info.getValue(),
      meta: {
        headerClassName: 'capitalize truncate',
      },
    }),
  );

  const rows = generateDataPreviewTableRows(groupedColumns);

  return <DataTable columns={tableColumns} data={rows} isLoading={isLoading} />;
}
