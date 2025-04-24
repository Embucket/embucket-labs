import { useParams } from '@tanstack/react-router';
import { Database } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card';
import {
  SidebarGroup,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from '@/components/ui/sidebar';
import type { QueryRecord } from '@/orval/models';
import { useGetQueries } from '@/orval/queries';

import { SQLEditor } from '../sql-editor';
import { SqlEditorRightPanelQueryItem } from './sql-editor-right-panel-query-item';

interface SqlEditorRightPanelQueriesProps {
  selectedQueryRecord?: QueryRecord;
  onSetSelectedQueryRecord: (queryRecord: QueryRecord) => void;
}

export const SqlEditorRightPanelQueries = ({
  selectedQueryRecord,
  onSetSelectedQueryRecord,
}: SqlEditorRightPanelQueriesProps) => {
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
          <SidebarGroupLabel className="text-nowrap">{dateLabel}</SidebarGroupLabel>
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
                      <SqlEditorRightPanelQueryItem
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
