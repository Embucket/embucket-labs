import { useParams } from '@tanstack/react-router';
import { Database } from 'lucide-react';

import { EmptyContainer } from '@/components/empty-container';
import { SidebarGroup, SidebarGroupLabel, SidebarMenu } from '@/components/ui/sidebar';
import type { QueryRecord } from '@/orval/models';
import { useGetQueries } from '@/orval/queries';

import { EditorRightPanelQuery } from './editor-right-panel-query/editor-right-panel-query';

export const EditorRightPanelQueries = () => {
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
              <EditorRightPanelQuery key={query.id} query={query} />
            ))}
          </SidebarMenu>
        </SidebarGroup>
      ))}
    </>
  );
};
