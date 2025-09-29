import { useParams } from '@tanstack/react-router';
import { SlidersHorizontal, X } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { useGetQueries } from '@/orval/queries';

import { useEditorPanelsState } from '../editor-panels-state-provider';
import { EditorRightPanelQueries } from './editor-right-panel-queries';
import { EditorRightPanelQueriesSkeleton } from './editor-right-panel-queries-skeleton';

export const EditorRightPanel = () => {
  const { toggleRightPanel } = useEditorPanelsState();
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });
  const { data: { items: queries } = {}, isFetching: isFetchingQueries } = useGetQueries(
    { worksheet_id: +worksheetId },
    { query: { enabled: worksheetId !== 'undefined' } },
  );

  return (
    <>
      <div className="flex h-13 items-center justify-between border-b px-4 text-sm">
        History
        <Button
          onClick={toggleRightPanel}
          size="icon"
          variant="ghost"
          className="text-muted-foreground size-8"
        >
          <X />
        </Button>
      </div>
      <div className="text-muted-foreground flex items-center justify-between px-4 py-2 text-sm text-nowrap">
        <p className="mr-2">{queries?.length ? `All queries (${queries.length})` : ''}</p>
        <Button disabled size="icon" variant="ghost" className="size-8">
          <SlidersHorizontal />
        </Button>
      </div>
      {/* TODO: Hardcode */}
      <ScrollArea className="h-[calc(100vh-136px)]">
        {isFetchingQueries ? <EditorRightPanelQueriesSkeleton /> : <EditorRightPanelQueries />}
        <ScrollBar orientation="vertical" />
      </ScrollArea>
    </>
  );
};
