import { useNavigate } from '@tanstack/react-router';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
import { formatTime } from '@/lib/formatTime';
import type { Worksheet } from '@/orval/models';

import { useSqlEditorSettingsStore } from '../sql-editor/sql-editor-settings-store';

interface HomeWorksheetsTableProps {
  isLoading: boolean;
  worksheets: Worksheet[];
}

export function HomeWorksheetsTable({ isLoading, worksheets }: HomeWorksheetsTableProps) {
  const navigate = useNavigate();
  const addTab = useSqlEditorSettingsStore((state) => state.addTab);

  const handleRowClick = (row: Worksheet) => {
    addTab(row);
    navigate({ to: `/sql-editor/$worksheetId`, params: { worksheetId: row.id.toString() } });
  };
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
      cell: (info) => {
        return <span>{formatTime(info.getValue())}</span>;
      },
    }),
    columnHelper.accessor('createdAt', {
      header: 'Created At',
      cell: (info) => {
        return <span>{formatTime(info.getValue())}</span>;
      },
    }),
  ];

  return (
    <DataTable
      rounded
      columns={columns}
      onRowClick={handleRowClick}
      data={worksheets}
      isLoading={isLoading}
    />
  );
}
