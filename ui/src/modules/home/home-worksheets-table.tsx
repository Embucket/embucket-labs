import { useNavigate } from '@tanstack/react-router';
import { createColumnHelper } from '@tanstack/react-table';

import { DataTable } from '@/components/data-table/data-table';
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
        const date = new Date(info.getValue()).toLocaleTimeString('en-US', {
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
        });
        return <span>{date}</span>;
      },
    }),
    columnHelper.accessor('createdAt', {
      header: 'Created At',
      cell: (info) => {
        const date = new Date(info.getValue()).toLocaleTimeString('en-US', {
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
        });
        return <span>{date}</span>;
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
