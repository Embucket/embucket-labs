import { Button } from '@/components/ui/button';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';

import { useSqlEditorPanelsState } from '../sql-editor-panels-state-provider';
import EditorTabs from '../sql-editor-tabs';

export const SqlEditorCenterPanelTabs = () => {
  const { isLeftPanelExpanded, isRightPanelExpanded } = useSqlEditorPanelsState();
  const initialTabs = [
    { id: '1', title: '2025-03-06 11:59:00', content: 'Date and time content' },
    { id: '2', title: 'TPC-DS 10TB Comparison', content: 'Comparison data content' },
    { id: '3', title: 'Benchmarking', content: 'Benchmarking results' },
    { id: '4', title: 'TPC-TR Query Test', content: 'Query test results' },
  ];

  const handleTabChange = (tabId: string) => {
    console.log(`Tab changed to: ${tabId}`);
  };

  const handleTabClose = (tabId: string) => {
    console.log(`Tab closed: ${tabId}`);
  };

  const handleTabAdd = () => {
    console.log('New tab added');
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
          <EditorTabs
            initialTabs={initialTabs}
            onTabChange={handleTabChange}
            onTabClose={handleTabClose}
            onTabAdd={handleTabAdd}
          />
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
