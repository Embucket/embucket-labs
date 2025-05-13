import { useNavigate } from '@tanstack/react-router';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
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
    columnHelper.accessor('totalBytes', {
      header: 'Total Bytes',
    }),
    columnHelper.accessor('totalRows', {
      header: 'Row Count',
    }),
    columnHelper.accessor('createdAt', {
      header: 'Created',
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
