import { useNavigate } from '@tanstack/react-router';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import { formatTime } from '@/lib/formatTime';
import type { Schema } from '@/orval/models';

interface SchemasTableProps {
  isLoading: boolean;
  schemas: Schema[];
}

export function SchemasTable({ isLoading, schemas }: SchemasTableProps) {
  const navigate = useNavigate();
  const columnHelper = createColumnHelper<Schema>();

  const columns = [
    columnHelper.accessor('name', {
      header: 'Name',
    }),
    columnHelper.accessor('createdAt', {
      header: 'Created',
      cell: (info) => {
        return <span>{formatTime(info.getValue())}</span>;
      },
    }),
  ];

  const handleRowClick = (row: Schema) => {
    navigate({
      to: `/databases/$databaseName/schemas/$schemaName/tables`,
      params: { databaseName: row.database, schemaName: row.name },
    });
  };

  return (
    <DataTable
      rounded
      columns={columns}
      data={schemas}
      isLoading={isLoading}
      onRowClick={handleRowClick}
    />
  );
}
