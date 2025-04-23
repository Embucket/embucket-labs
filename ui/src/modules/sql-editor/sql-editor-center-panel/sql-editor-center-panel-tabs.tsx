import { Button } from '@/components/ui/button';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';
import { useGetWorksheets } from '@/orval/worksheets';

import { useSqlEditorPanelsState } from '../sql-editor-panels-state-provider';
import EditorTabs from '../sql-editor-tabs';

export const SqlEditorCenterPanelTabs = () => {
  const { isLeftPanelExpanded, isRightPanelExpanded } = useSqlEditorPanelsState();
  const { data: { items: worksheets } = {}, isFetching } = useGetWorksheets();

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
          {!isFetching && <EditorTabs worksheets={worksheets} />}
          <ScrollBar orientation="horizontal" />
        </ScrollArea>
      </div>
      <Button
        variant="outline"
        size="icon"
        className="mt-auto mr-4 size-9 rounded-tl-md rounded-tr-md rounded-b-none border-b-0 transition-all"
      >
        +
      </Button>
    </div>
  );
};
