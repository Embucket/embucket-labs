import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import { formatTime } from '@/lib/formatTime';
import type { Volume } from '@/orval/models';

interface VolumesTableProps {
  isLoading: boolean;
  volumes: Volume[];
}

export function VolumesTable({ isLoading, volumes }: VolumesTableProps) {
  const columnHelper = createColumnHelper<Volume>();

  const tableColumns = [
    columnHelper.accessor('name', {
      header: 'Name',
    }),
    columnHelper.accessor('type', {
      header: 'Type',
    }),
    columnHelper.accessor('createdAt', {
      header: 'Created',
      cell: (info) => {
        return <span>{formatTime(info.getValue())}</span>;
      },
    }),
  ];

  return <DataTable rounded columns={tableColumns} data={volumes} isLoading={isLoading} />;
}
