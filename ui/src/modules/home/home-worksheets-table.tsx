import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import type { Worksheet } from '@/orval/models';

interface HomeWorksheetsTableProps {
  isLoading: boolean;
  worksheets: Worksheet[];
}

export function HomeWorksheetsTable({ isLoading, worksheets }: HomeWorksheetsTableProps) {
  const columnHelper = createColumnHelper<Worksheet>();

  const columns = [
    columnHelper.accessor('id', {
      header: 'Id',
    }),
    columnHelper.accessor('content', {
      header: 'Content',
    }),
    columnHelper.accessor('updatedAt', {
      header: 'Updated At',
    }),
    columnHelper.accessor('createdAt', {
      header: 'Created At',
    }),
  ];

  return <DataTable rounded columns={columns} data={worksheets} isLoading={isLoading} />;
}
