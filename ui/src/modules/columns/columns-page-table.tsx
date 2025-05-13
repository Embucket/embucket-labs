import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import type { TableColumn } from '@/orval/models';

interface ColumnsTableProps {
  isLoading: boolean;
  columns: TableColumn[];
}

export function ColumnsTable({ isLoading, columns }: ColumnsTableProps) {
  const columnHelper = createColumnHelper<TableColumn>();

  const tableColumns = [
    columnHelper.accessor('name', {
      header: 'Name',
    }),
    columnHelper.accessor('type', {
      header: 'Type',
    }),
  ];

  return <DataTable rounded columns={tableColumns} data={columns} isLoading={isLoading} />;
}
