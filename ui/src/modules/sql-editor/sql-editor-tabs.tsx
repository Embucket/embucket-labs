import { useState } from 'react';

import { Link, useNavigate, useParams } from '@tanstack/react-router';
import { SquareTerminal, X } from 'lucide-react';

import { cn } from '@/lib/utils';
import type { Worksheet } from '@/orval/models';

interface EditorTabsProps {
  worksheets: Worksheet[];
  onTabChange?: (tabId: number) => void;
  onTabClose?: (tabId: number) => void;
  onTabAdd?: () => void;
}

export default function EditorTabs({
  worksheets = [],
  onTabChange,
  onTabClose,
  onTabAdd,
}: EditorTabsProps) {
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });
  const navigate = useNavigate();

  const [tabs, setTabs] = useState<Worksheet[]>([
    worksheets.find((w) => w.id === +worksheetId) ?? worksheets[0],
  ]);

  const handleTabClose = (e: React.MouseEvent, tabId: number) => {
    e.stopPropagation();
    e.preventDefault();

    const newTabs = tabs.filter((tab) => tab.id !== tabId);
    setTabs(newTabs);

    // If we closed the active tab, activate the first available tab
    if (+worksheetId === tabId && newTabs.length > 0) {
      onTabChange?.(newTabs[0].id);
    }

    onTabClose?.(tabId);
  };

  // const handleAddTab = () => {
  //   const newTabId = `tab-${Date.now()}`;
  //   const newTab = { id: newTabId, title: 'New Tab' };
  //   setTabs([...tabs, newTab]);
  //   setActiveTab(newTabId);
  //   onTabChange?.(newTabId);
  //   onTabAdd?.();
  // };

  return (
    <div className="relative ml-4 flex items-center gap-1">
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
                  'text-muted-foreground mr-2 size-4 justify-start',
                  isActive && 'text-primary-foreground',
                )}
              />
              <span className="max-w-28 truncate">{tab.name}</span>
              <button
                onClick={(e) => handleTabClose(e, tab.id)}
                className="ml-auto rounded-sm p-0.5 hover:bg-[#333333]"
              >
                <X size={14} />
              </button>
            </div>
          )}
        </Link>
      ))}
      {/* <button
        onClick={handleAddTab}
        className="fixed right-8 flex h-10 w-10 items-center justify-center bg-red-500 text-gray-400 hover:bg-[#1e1e1e] hover:text-gray-300"
      >
        <Plus size={18} />
      </button> */}
    </div>
  );
}
