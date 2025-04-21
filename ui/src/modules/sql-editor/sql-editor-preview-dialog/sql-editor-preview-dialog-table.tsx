import type { ColumnDef } from '@tanstack/react-table';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import type { TablePreviewDataColumn } from '@/orval/models';

interface SqlEditorPreviewDialogTableProps {
  isLoading: boolean;
  columns: TablePreviewDataColumn[];
}

export function SqlEditorPreviewDialogTable({
  isLoading,
  columns,
}: SqlEditorPreviewDialogTableProps) {
  const columnHelper = createColumnHelper<Record<string, string>>();

  const tableColumns: ColumnDef<Record<string, string>, string>[] = columns.map((column) =>
    columnHelper.accessor((row) => row[column.name], {
      header: column.name,
      cell: (info) => info.getValue(),
      meta: {
        headerClassName: 'capitalize max-w-40 truncate',
      },
    }),
  );

  const rows =
    columns.length > 0
      ? columns[0].rows.map((_, index) =>
          columns.reduce<Record<string, string>>((acc, column) => {
            acc[column.name] = column.rows[index]?.data || '';
            return acc;
          }, {}),
        )
      : [];

  return <DataTable columns={tableColumns} data={rows} isLoading={isLoading} />;
}
