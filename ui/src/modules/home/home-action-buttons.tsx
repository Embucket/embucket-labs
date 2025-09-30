import { useState } from 'react';

import { useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';
import { Database, Plus, Upload, type LucideIcon } from 'lucide-react';

import { TableDataUploadDialog } from '@/modules/shared/table-data-upload-dialog/table-data-upload-dialog';
import { getGetWorksheetsQueryKey, useCreateWorksheet } from '@/orval/worksheets';

import { useEditorSettingsStore } from '../editor/editor-settings-store';
import { CreateDatabaseDialog } from '../shared/create-database-dialog/create-database-dialog';

interface HomeActionButtonProps {
  icon: LucideIcon;
  label: string;
  onClick: () => void;
  disabled?: boolean;
}

function HomeActionButton({ icon: Icon, label, onClick, disabled = false }: HomeActionButtonProps) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className="hover:bg-hover bg-muted flex h-[70px] cursor-pointer items-center gap-3 rounded-md border p-6 text-white transition-colors disabled:bg-transparent"
    >
      <Icon className="text-muted-foreground size-5" />
      <span className="text-sm font-medium">{label}</span>
    </button>
  );
}

interface HomeActionButtonsProps {
  isLoading: boolean;
}

export default function HomeActionButtons({ isLoading }: HomeActionButtonsProps) {
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
        <div className="grid grid-cols-3 gap-4">
          <HomeActionButton
            icon={Plus}
            label="Create SQL Worksheet"
            onClick={handleCreateWorksheet}
            disabled={isPending || isLoading}
          />

          <HomeActionButton
            icon={Database}
            label="Create Database"
            onClick={() => setOpened(true)}
            disabled={isLoading}
          />

          <HomeActionButton
            icon={Upload}
            label="Upload Local Files"
            onClick={() => setIsUploadFileDialogOpened(true)}
            disabled={isLoading}
          />
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
