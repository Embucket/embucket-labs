import { useState } from 'react';

import { useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';
import { Database, Plus, Upload } from 'lucide-react';

import { getGetWorksheetsQueryKey, useCreateWorksheet } from '@/orval/worksheets';

import { useSqlEditorSettingsStore } from '../sql-editor/sql-editor-settings-store';
import { CreateDatabaseDialog } from './create-database-dialog';

export default function HomeActionButtons() {
  const [opened, setOpened] = useState(false);
  const addTab = useSqlEditorSettingsStore((state) => state.addTab);

  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const { mutateAsync, isPending } = useCreateWorksheet({
    mutation: {
      onSuccess: (worksheet) => {
        addTab(worksheet);
        navigate({
          to: '/sql-editor/$worksheetId',
          params: {
            worksheetId: worksheet.id.toString(),
          },
        });
        queryClient.invalidateQueries({
          queryKey: getGetWorksheetsQueryKey(),
        });
      },
    },
  });

  const handleCreateWorksheet = () => {
    mutateAsync({
      data: {
        name: '',
        content: '',
      },
    });
  };

  return (
    <>
      <div className="w-full p-4 pb-0">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
          <button
            onClick={handleCreateWorksheet}
            disabled={isPending}
            className="hover:bg-sidebar-secondary-accent bg-muted flex cursor-pointer items-center gap-3 rounded-md border p-6 text-white transition-colors"
          >
            <Plus className="text-muted-foreground size-5" />
            <span className="text-sm font-medium">Create SQL Worksheet</span>
          </button>

          <button
            onClick={() => setOpened(true)}
            className="hover:bg-sidebar-secondary-accent bg-muted flex cursor-pointer items-center gap-3 rounded-md border p-6 text-white transition-colors"
          >
            <Database className="text-muted-foreground size-5" />
            <span className="text-sm font-medium">Create Database</span>
          </button>

          <button
            disabled
            className="hover:bg-sidebar-secondary-accent bg-muted flex cursor-pointer items-center gap-3 rounded-md border p-6 text-white transition-colors disabled:cursor-auto disabled:bg-transparent disabled:hover:bg-transparent"
          >
            <Upload className="text-muted-foreground size-5" />
            <span className="text-sm font-medium">Upload Local Files</span>
          </button>
        </div>
      </div>
      <CreateDatabaseDialog opened={opened} onSetOpened={setOpened} />
    </>
  );
}
