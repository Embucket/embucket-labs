import { useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';

import { Button } from '@/components/ui/button';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { getGetWorksheetsQueryKey, useCreateWorksheet } from '@/orval/worksheets';

import { useSqlEditorSettingsStore } from '../../sql-editor-settings-store';
import { SqlEditorCenterPanelHeaderTabs } from './sql-editor-center-panel-header-tabs';

export const SqlEditorCenterPanelHeader = () => {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const addTab = useSqlEditorSettingsStore((state) => state.addTab);

  const { mutateAsync, isPending } = useCreateWorksheet({
    mutation: {
      onSuccess: (worksheet) => {
        queryClient.invalidateQueries({
          queryKey: getGetWorksheetsQueryKey(),
        });
        navigate({
          to: '/sql-editor/$worksheetId',
          params: {
            worksheetId: worksheet.id.toString(),
          },
        });
      },
    },
  });

  const handleAddTab = async () => {
    const worksheet = await mutateAsync({
      data: {
        name: '',
        content: '',
      },
    });
    addTab(worksheet);
    navigate({
      to: '/sql-editor/$worksheetId',
      params: {
        worksheetId: worksheet.id.toString(),
      },
    });
  };

  return (
    <div className="flex min-h-13 items-center gap-1 border-b pl-4">
      {/* TODO: Hardcode */}
      <div className="mt-auto max-w-[calc(100%-4px-16px-36px)]">
        <ScrollArea className="mt-auto flex size-full min-w-full flex-col">
          <SqlEditorCenterPanelHeaderTabs />
          <ScrollBar orientation="horizontal" />
        </ScrollArea>
      </div>
      <Button
        disabled={isPending}
        onClick={handleAddTab}
        variant="outline"
        size="icon"
        className="hover:bg-sidebar-secondary-accent! mt-auto mr-4 size-9 rounded-tl-md rounded-tr-md rounded-b-none border-b-0 border-none transition-all"
      >
        +
      </Button>
    </div>
  );
};
