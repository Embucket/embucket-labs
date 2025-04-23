import { useState } from 'react';

import { Plus, SquareTerminal, X } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

interface Tab {
  id: string;
  title: string;
  content?: React.ReactNode;
}

interface EditorTabsProps {
  initialTabs?: Tab[];
  onTabChange?: (tabId: string) => void;
  onTabClose?: (tabId: string) => void;
  onTabAdd?: () => void;
}

export default function EditorTabs({
  initialTabs = [],
  onTabChange,
  onTabClose,
  onTabAdd,
}: EditorTabsProps) {
  const [tabs, setTabs] = useState<Tab[]>(
    initialTabs.length > 0
      ? initialTabs
      : [
          { id: '1', title: '2025-03-06 11:59:00' },
          { id: '2', title: 'TPC-DS 10TB Comparison' },
          { id: '3', title: 'Benchmarking' },
          { id: '4', title: 'TPC-TR Query Test' },
        ],
  );
  const [activeTab, setActiveTab] = useState<string>(tabs[0]?.id || '');

  const handleTabChange = (tabId: string) => {
    setActiveTab(tabId);
    onTabChange?.(tabId);
  };

  const handleTabClose = (e: React.MouseEvent, tabId: string) => {
    e.stopPropagation();
    const newTabs = tabs.filter((tab) => tab.id !== tabId);
    setTabs(newTabs);

    // If we closed the active tab, activate the first available tab
    if (activeTab === tabId && newTabs.length > 0) {
      setActiveTab(newTabs[0].id);
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
      {tabs.map((tab, inddex) => (
        <Button
          key={tab.id}
          onClick={() => handleTabChange(tab.id)}
          className={cn(
            'relative flex w-[180px] items-center self-end rounded-tl-md rounded-tr-md rounded-b-none border border-b-0 bg-[#1e1e1e1]',
            'hover:bg-sidebar-secondary-accent',
            activeTab === tab.id ? 'bg-sidebar-secondary-accent' : '',
          )}
        >
          <SquareTerminal className="text-muted-foreground mr-2 size-4" />
          <span className="max-w-28 truncate">{tab.title}</span>
          <button
            onClick={(e) => handleTabClose(e, tab.id)}
            className="rounded-sm p-0.5 hover:bg-[#333333]"
          >
            <X size={14} />
          </button>
        </Button>
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
