import { useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';

import { Button } from '@/components/ui/button';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';
import { getGetWorksheetsQueryKey, useCreateWorksheet } from '@/orval/worksheets';

import { useSqlEditorPanelsState } from '../sql-editor-panels-state-provider';
import { useSqlEditorSettingsStore } from '../sql-editor-settings-store';
import EditorTabs from '../sql-editor-tabs';

export const SqlEditorCenterPanelTabs = () => {
  const { isLeftPanelExpanded, isRightPanelExpanded } = useSqlEditorPanelsState();

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
    <div className="flex items-center gap-1 border-b">
      <div className="flex min-h-13 flex-col">
        <ScrollArea
          className={cn(
            'mt-auto flex min-w-full flex-col transition-all duration-500',
            (isLeftPanelExpanded || isRightPanelExpanded) &&
              'max-w-[calc(100vw-256px-8px-256px-36px-24px-15px)]',
            isLeftPanelExpanded &&
              isRightPanelExpanded &&
              'max-w-[calc(100vw-256px-8px-256px-36px-24px-256px-14px)]',
          )}
        >
          <EditorTabs />
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
