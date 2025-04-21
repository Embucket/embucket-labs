import type { ColumnDef } from '@tanstack/react-table';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import type { Column, Row } from '@/orval/models';

interface QueryResultDataTableProps {
  isLoading: boolean;
  rows: Row[];
  columns: Column[];
}

export function QueryResultDataTable({ isLoading, rows, columns }: QueryResultDataTableProps) {
  const columnHelper = createColumnHelper<unknown[]>();

  const tableColumns: ColumnDef<Row, string>[] = [
    ...columns,
    { name: 'id', type: 'VARCHAR(255)', nullable: 'true', default: 'NULL', description: '' },
    { name: 'email', type: 'VARCHAR(255)', nullable: 'true', default: 'NULL', description: '' },
    { name: 'amount', type: 'NUMBER(38, 10)', nullable: 'true', default: 'NULL', description: '' },
    { name: 'date', type: 'DATE', nullable: 'true', default: 'NULL', description: '' },
    {
      name: 'descriptiondescriptiondescriptiondescriptiondescriptiondescriptiondescriptiondescriptiondescriptiondescription',
      type: 'VARCHAR(255)',
      nullable: 'true',
      default: 'NULL',
      description: '',
    },
  ].map((column) =>
    columnHelper.accessor((row) => row[columns.indexOf(column)], {
      header: column.name,
      cell: (info) => info.getValue(),
      meta: {
        headerClassName: 'capitalize',
      },
    }),
  );

  return <DataTable removeXBorders columns={tableColumns} data={rows} isLoading={isLoading} />;
}
