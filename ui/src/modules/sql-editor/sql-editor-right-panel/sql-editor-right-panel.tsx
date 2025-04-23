import { useParams } from '@tanstack/react-router';
import { Database, SlidersHorizontal, X } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { Button } from '@/components/ui/button';
import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import {
  SidebarGroup,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from '@/components/ui/sidebar';
import { cn } from '@/lib/utils';
import type { QueryRecord } from '@/orval/models';
import { useGetQueries } from '@/orval/queries';

import { SQLEditor } from '../sql-editor';
import { useSqlEditorPanelsState } from '../sql-editor-panels-state-provider';

// const DATA: QueryRecord[] = [
//   {
//     id: 1,
//     query: 'SELECT * FROM users',
//     error: '',
//     result: {
//       columns: [
//         { name: 'id', type: 'INT' },
//         { name: 'name', type: 'VARCHAR(255)' },
//       ],
//       rows: [
//         [1, 'John'],
//         [2, 'Charlie'],
//         [3, 'Bob'],
//       ],
//     },
//     resultCount: 0,
//     startTime: '11:55:00 AM',
//     endTime: '2023-10-01T12:00:05Z',
//     durationMs: 5000,
//     status: 'successful',
//     worksheetId: 1,
//   },
//   {
//     id: 2,
//     query: 'SELECT * FROM orders',
//     error: '',
//     result: {
//       columns: [],
//       rows: [],
//     },
//     resultCount: 0,
//     startTime: '12:00:05 AM',
//     endTime: '2023-10-01T12:05:10Z',
//     durationMs: 10000,
//     status: 'failed',
//     worksheetId: 1,
//   },
// ];

interface QueryItemProps {
  status: QueryRecord['status'];
  query: string;
  time: string;
}

function QueryItem({ status, query, time }: QueryItemProps) {
  return (
    <li className="group flex w-full items-center justify-between">
      <div className="flex items-center gap-2">
        <span
          className={cn(
            'mx-2 size-1 rounded-full',
            status === 'successful' ? 'bg-green-500' : 'bg-red-500',
          )}
        />
        <span className="max-w-[124px] truncate text-sm">{query}</span>
      </div>
      <span className="text-muted-foreground text-xs">{time}</span>
    </li>
  );
}

interface SqlEditorSidebarQueriesProps {
  selectedQueryRecord?: QueryRecord;
  onSetSelectedQueryRecord: (queryRecord: QueryRecord) => void;
}

const SqlEditorSidebarQueries = ({
  selectedQueryRecord,
  onSetSelectedQueryRecord,
}: SqlEditorSidebarQueriesProps) => {
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });

  const { data: { items: queries } = {} } = useGetQueries(
    { worksheetId: +worksheetId },
    { query: { enabled: worksheetId !== 'undefined' } },
  );

  if (!queries?.length) {
    return (
      <EmptyContainer
        className="absolute text-center text-wrap"
        Icon={Database}
        title="No Queries Yet"
        description="Your query history will appear here once you run a query."
      />
    );
  }

  const groupedQueries = queries.reduce<Record<string, QueryRecord[]>>((acc, query) => {
    const dateLabel = new Date(query.startTime).toDateString();
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (!acc[dateLabel]) {
      acc[dateLabel] = [];
    }
    acc[dateLabel].push(query);
    return acc;
  }, {});

  return (
    <>
      {Object.entries(groupedQueries).map(([dateLabel, queries]) => (
        <SidebarGroup key={dateLabel}>
          <SidebarGroupLabel>{dateLabel}</SidebarGroupLabel>
          <SidebarMenu>
            {queries.map((query) => (
              <HoverCard key={query.id} openDelay={100} closeDelay={10}>
                <HoverCardTrigger>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      isActive={query.id === selectedQueryRecord?.id}
                      onClick={() => onSetSelectedQueryRecord(query)}
                      className="hover:bg-sidebar-secondary-accent data-[active=true]:bg-sidebar-secondary-accent! data-[active=true]:font-light"
                    >
                      <QueryItem
                        status={query.status}
                        query={query.query}
                        time={new Date(query.startTime).toLocaleTimeString('en-US', {
                          hour: '2-digit',
                          minute: '2-digit',
                          second: '2-digit',
                        })}
                      />
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                </HoverCardTrigger>
                <HoverCardContent className="p-1">
                  <div className="rounded bg-[#1F1F1F]">
                    <SQLEditor readonly content={query.query} />
                  </div>
                </HoverCardContent>
              </HoverCard>
            ))}
          </SidebarMenu>
        </SidebarGroup>
      ))}
    </>
  );
};

interface SqlEditorRightPanelProps {
  selectedQueryRecord?: QueryRecord;
  onSetSelectedQueryRecord: (queryRecord: QueryRecord) => void;
}

export const SqlEditorRightPanel = ({
  selectedQueryRecord,
  onSetSelectedQueryRecord,
}: SqlEditorRightPanelProps) => {
  const { isRightPanelExpanded, toggleRightPanel } = useSqlEditorPanelsState();
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });
  const { data: { items: queries } = {} } = useGetQueries(
    { worksheetId: +worksheetId },
    { query: { enabled: worksheetId !== 'undefined' } },
  );

  return (
    <div
      className={cn(
        'size-full border-l text-nowrap transition-all duration-500',
        isRightPanelExpanded ? 'max-w-[256px] opacity-100' : 'max-w-0 opacity-0',
      )}
    >
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
      <div className="text-muted-foreground flex items-center justify-between px-4 py-2 text-sm">
        All queries ({queries?.length})
        <Button disabled size="icon" variant="ghost" className="size-8">
          <SlidersHorizontal />
        </Button>
      </div>
      {/* TODO: No hardcode */}
      <ScrollArea className="h-[calc(100vh-136px)]">
        <SqlEditorSidebarQueries
          selectedQueryRecord={selectedQueryRecord}
          onSetSelectedQueryRecord={onSetSelectedQueryRecord}
        />
        <ScrollBar orientation="vertical" />
      </ScrollArea>
    </div>
  );
};
