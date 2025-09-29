import { useNavigate } from '@tanstack/react-router';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import { formatTime } from '@/lib/formatTime';
import type { Database } from '@/orval/models';

interface DatabasesTableProps {
  isLoading: boolean;
  databases: Database[];
}

export function DatabasesTable({ isLoading, databases }: DatabasesTableProps) {
  const navigate = useNavigate();
  const columnHelper = createColumnHelper<Database>();

  const columns = [
    columnHelper.accessor('name', {
      header: 'Name',
    }),
    columnHelper.accessor('volume', {
      header: 'Volume',
    }),
    columnHelper.accessor('createdAt', {
      header: 'Created',
      cell: (info) => {
        return <span>{formatTime(info.getValue())}</span>;
      },
    }),
  ];

  const handleRowClick = (row: Database) => {
    navigate({ to: `/databases/$databaseName/schemas`, params: { databaseName: row.name } });
  };

  return (
    <DataTable
      rounded
      columns={columns}
      data={databases}
      isLoading={isLoading}
      onRowClick={handleRowClick}
    />
  );
}
