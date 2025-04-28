import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { SidebarMenuButton, SidebarMenuItem } from '@/components/ui/sidebar';
import type { QueryRecord } from '@/orval/models';

import { SQLEditor } from '../../sql-editor';
import { useSqlEditorSettingsStore } from '../../sql-editor-settings-store';
import { SqlEditorRightPanelQueryCopyButton } from './sql-editor-right-panel-query-copy-button';
import { SqlEditorRightPanelQueryItem } from './sql-editor-right-panel-query-item';

interface SqlEditorRightPanelQueriesProps {
  query: QueryRecord;
}

export const SqlEditorRightPanelQuery = ({ query }: SqlEditorRightPanelQueriesProps) => {
  const queryRecord = useSqlEditorSettingsStore((state) => state.queryRecord);
  const setQueryRecord = useSqlEditorSettingsStore((state) => state.setQueryRecord);

  return (
    <HoverCard key={query.id} openDelay={100} closeDelay={10}>
      <HoverCardTrigger>
        <SidebarMenuItem>
          <SidebarMenuButton
            isActive={query.id === queryRecord?.id}
            onClick={() => setQueryRecord(query)}
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
      <HoverCardContent className="flex size-full max-h-[200px] max-w-[400px] flex-1 flex-col p-1">
        <div className="rounded bg-[#1F1F1F]">
          <div className="flex items-center justify-end p-2 pb-0">
            <SqlEditorRightPanelQueryCopyButton query={query} />
          </div>
          {/* TODO: Hardcode */}
          <ScrollArea className="p-2 [&>[data-radix-scroll-area-viewport]]:max-h-[calc(200px-16px-36px-12px)]">
            <SQLEditor readonly content={query.query} />
            <ScrollBar orientation="vertical" />
          </ScrollArea>
        </div>
      </HoverCardContent>
    </HoverCard>
  );
};
