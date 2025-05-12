import { useNavigate } from '@tanstack/react-router';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import dayjs from '@/lib/dayjs';
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
    columnHelper.accessor('created_at', {
      header: 'Start Time',
      cell: (info) => {
        const startTime = dayjs(info.getValue());
        const diff = dayjs().diff(startTime, 'minute');
        const date =
          diff < 24 ? dayjs(startTime).fromNow() : dayjs(startTime).format('DD/MM/YYYY HH:mm');
        return <span>{date}</span>;
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
