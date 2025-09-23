import { useState } from 'react';

import { useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';
import { Database, Plus, Upload } from 'lucide-react';

import { TableDataUploadDialog } from '@/modules/shared/table-data-upload-dialog/table-data-upload-dialog';
import { getGetWorksheetsQueryKey, useCreateWorksheet } from '@/orval/worksheets';

import { useEditorSettingsStore } from '../editor/editor-settings-store';
import { CreateDatabaseDialog } from '../shared/create-database-dialog/create-database-dialog';

export default function HomeActionButtons() {
  const [opened, setOpened] = useState(false);
  const [isUploadFileDialogOpened, setIsUploadFileDialogOpened] = useState(false);
  const addTab = useEditorSettingsStore((state) => state.addTab);

  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const { mutate, isPending } = useCreateWorksheet({
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
    mutate({
      data: {
        name: '',
        content: '',
      },
    });
  };

  return (
    <>
      <div className="mt-4 w-full px-4">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
          <button
            onClick={handleCreateWorksheet}
            disabled={isPending}
            className="hover:bg-hover bg-muted flex cursor-pointer items-center gap-3 rounded-md border p-6 text-white transition-colors"
          >
            <Plus className="text-muted-foreground size-5" />
            <span className="text-sm font-medium">Create SQL Worksheet</span>
          </button>

          <button
            onClick={() => setOpened(true)}
            className="hover:bg-hover bg-muted flex cursor-pointer items-center gap-3 rounded-md border p-6 text-white transition-colors"
          >
            <Database className="text-muted-foreground size-5" />
            <span className="text-sm font-medium">Create Database</span>
          </button>

          <button
            onClick={() => setIsUploadFileDialogOpened(true)}
            className="hover:bg-hover bg-muted flex cursor-pointer items-center gap-3 rounded-md border p-6 text-white transition-colors"
          >
            <Upload className="text-muted-foreground size-5" />
            <span className="text-sm font-medium">Upload Local Files</span>
          </button>
        </div>
      </div>
      <CreateDatabaseDialog opened={opened} onSetOpened={setOpened} />
      <TableDataUploadDialog
        opened={isUploadFileDialogOpened}
        onSetOpened={setIsUploadFileDialogOpened}
      />
    </>
  );
}
