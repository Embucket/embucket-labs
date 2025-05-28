import { useNavigate } from '@tanstack/react-router';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import { formatTime } from '@/lib/formatTime';
import type { Table } from '@/orval/models';

interface TablesTableProps {
  isLoading: boolean;
  tables: Table[];
  databaseName: string;
  schemaName: string;
}

export function TablesTable({ isLoading, tables, databaseName, schemaName }: TablesTableProps) {
  const navigate = useNavigate();
  const columnHelper = createColumnHelper<Table>();

  const columns = [
    columnHelper.accessor('name', {
      header: 'Name',
    }),
    columnHelper.accessor('type', {
      header: 'Type',
    }),
    columnHelper.accessor('totalRows', {
      header: 'Rows',
    }),
    columnHelper.accessor('totalBytes', {
      header: 'Bytes',
    }),
    columnHelper.accessor('createdAt', {
      header: 'Created',
      cell: (info) => {
        return <span>{formatTime(info.getValue())}</span>;
      },
    }),
  ];

  const handleRowClick = (row: Table) => {
    navigate({
      to: `/databases/$databaseName/schemas/$schemaName/tables/$tableName/columns`,
      params: { databaseName, schemaName, tableName: row.name },
    });
  };

  return (
    <DataTable
      rounded
      columns={columns}
      data={tables}
      isLoading={isLoading}
      onRowClick={handleRowClick}
    />
  );
}
