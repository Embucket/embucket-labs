import { Link } from '@tanstack/react-router';
import { SquareTerminal } from 'lucide-react';

import { cn } from '@/lib/utils';

import { useEditorSettingsStore } from '../../editor-settings-store';
import { EditorCenterPanelHeaderTabsCloseButton } from './editor-center-panel-header-tabs-close-button';

export function EditorCenterPanelHeaderTabs() {
  const tabs = useEditorSettingsStore((state) => state.tabs);
  const createQueryPending = useEditorSettingsStore((state) => state.createQueryPending);

  return (
    <div className="relative flex items-center gap-1">
      {tabs.map((tab) => (
        <Link
          key={tab.id}
          disabled={createQueryPending}
          to="/sql-editor/$worksheetId"
          params={{ worksheetId: tab.id.toString() }}
          activeOptions={{
            includeSearch: false,
          }}
        >
          {({ isActive }) => (
            <div
              className={cn(
                'bg-muted relative flex h-9 w-[180px] items-center self-end rounded-tl-md rounded-tr-md rounded-b-none border border-b-0 px-3 text-xs',
                !createQueryPending && 'hover:bg-hover',
                isActive
                  ? 'text-primary-foreground bg-transparent hover:bg-transparent'
                  : 'border-none',
              )}
            >
              <SquareTerminal
                className={cn(
                  'text-muted-foreground mr-2 size-4 min-h-4 min-w-4 justify-start',
                  isActive && 'text-primary-foreground',
                )}
              />
              <span className="mr-2 max-w-28 truncate">{tab.name}</span>
              <EditorCenterPanelHeaderTabsCloseButton disabled={createQueryPending} tab={tab} />
            </div>
          )}
        </Link>
      ))}
    </div>
  );
}
