import { Link, useNavigate, useParams } from '@tanstack/react-router';
import { SquareTerminal, X } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import type { Worksheet } from '@/orval/models';

import { useSqlEditorSettingsStore } from '../../sql-editor-settings-store';

export function SqlEditorCenterPanelHeaderTabs() {
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });

  const navigate = useNavigate();
  const tabs = useSqlEditorSettingsStore((state) => state.tabs);
  const removeTab = useSqlEditorSettingsStore((state) => state.removeTab);

  const handleTabClose = (e: React.MouseEvent, tab: Worksheet) => {
    e.stopPropagation();
    e.preventDefault();
    const tabIndex = tabs.findIndex((t) => t.id === tab.id);
    removeTab(tab);

    if (tabs.length === 1) {
      navigate({ to: '/home' });
      return;
    }
    if (tabIndex === 0 && tabs.length > 1) {
      // If the first tab is closed, navigate to the next tab
      navigate({
        to: '/sql-editor/$worksheetId',
        params: { worksheetId: tabs[1]?.id.toString() },
      });
    } else if (tabs.length > 1 && tab.id.toString() === worksheetId) {
      // Otherwise, navigate to the first tab
      navigate({
        to: '/sql-editor/$worksheetId',
        params: { worksheetId: tabs[0]?.id.toString() },
      });
    }
  };

  return (
    <div className="relative flex items-center gap-1">
      {tabs.map((tab) => (
        <Link
          key={tab.id}
          to="/sql-editor/$worksheetId"
          params={{ worksheetId: tab.id.toString() }}
        >
          {({ isActive }) => (
            <div
              className={cn(
                'bg-muted relative flex h-9 w-[180px] items-center self-end rounded-tl-md rounded-tr-md rounded-b-none border border-b-0 px-3 text-xs',
                'hover:bg-sidebar-secondary-accent',
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
              <Button
                variant="ghost"
                onClick={(e) => handleTabClose(e, tab)}
                className="ml-auto size-6"
              >
                <X />
              </Button>
            </div>
          )}
        </Link>
      ))}
    </div>
  );
}
