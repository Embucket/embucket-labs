import { useState } from 'react';

import { Navigate, useParams } from '@tanstack/react-router';
import { RefreshCw, Search } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input, InputIcon, InputRoot } from '@/components/ui/input';
import { ResizableHandle, ResizablePanelGroup } from '@/components/ui/resizable';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import {
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
} from '@/components/ui/sidebar';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { cn } from '@/lib/utils';
import type { Worksheet } from '@/orval/models';
import { useGetNavigationTrees } from '@/orval/navigation-trees';
import { useGetWorksheets } from '@/orval/worksheets';

import { useSqlEditorPanelsState } from '../sql-editor-panels-state-provider';
import { SqlEditorResizablePanel } from '../sql-editor-resizable';
import type { SelectedTree } from './sql-editor-left-panel-databases';
import { SqlEditorLeftPanelDatabases } from './sql-editor-left-panel-databases';
import { DATA as DATA_NAVIGATION_TREES } from './sql-editor-left-panel-databases-data';
import { SqlEditorLeftPanelTableFields } from './sql-editor-left-panel-table-fields';
import { SqlEditorLeftPanelWorksheets } from './sql-editor-left-panel-worksheets';

const DATA: Worksheet[] = [
  {
    content: 'SELECT * FROM users',
    createdAt: '2023-10-01T12:00:00Z',
    id: 1,
    name: 'Users',
    updatedAt: '2023-10-01T12:00:05Z',
  },
  {
    content: 'SELECT * FROM orders',
    createdAt: '2023-10-01T12:05:00Z',
    id: 2,
    name: 'Orders',
    updatedAt: '2023-10-01T12:05:10Z',
  },
];

export const SqlEditorLeftPanel = () => {
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });

  const {
    data: { items: navigationTrees = DATA_NAVIGATION_TREES } = {},
    refetch: refetchNavigationTrees,
    isFetching: isFetchingNavigationTrees,
  } = useGetNavigationTrees();
  const {
    data: { items: worksheets = DATA } = {},
    refetch: refetchWorksheets,
    isFetching: isFetchingWorksheets,
  } = useGetWorksheets();

  const [selectedNavigationTreeDatabase, setSelectedNavigationTreeDatabase] =
    useState<SelectedTree>();

  const {
    isLeftPanelExpanded,
    leftBottomRef,
    setIsResizing,
    setLeftBottomPanelExpanded,
    isLeftBottomPanelExpanded,
  } = useSqlEditorPanelsState();

  const [isRefreshing, setIsRefreshing] = useState(false);

  const handleRefresh = async () => {
    setIsRefreshing(true);
    await new Promise((resolve) => setTimeout(resolve, 500));
    refetchNavigationTrees();
    refetchWorksheets();
    setIsRefreshing(false);
  };

  if (!isFetchingWorksheets && worksheets.length && worksheetId === 'undefined') {
    return (
      <Navigate
        to="/sql-editor/$worksheetId"
        params={{ worksheetId: worksheets[0].id.toString() }}
      />
    );
  }

  return (
    <div
      className={cn(
        'size-full border-r text-nowrap transition-all duration-500',
        isLeftPanelExpanded ? 'max-w-[256px] opacity-100' : 'max-w-0 opacity-0',
      )}
    >
      <Tabs defaultValue="databases" className="h-full gap-0 text-nowrap">
        <SidebarHeader className="h-[60px] p-4 pb-2">
          <TabsList className="w-full text-nowrap">
            <TabsTrigger value="databases">Databases</TabsTrigger>
            <TabsTrigger value="worksheets">Worksheets</TabsTrigger>
          </TabsList>
        </SidebarHeader>

        <SidebarContent className="gap-0">
          {!!navigationTrees.length && (
            <SidebarGroup className="px-4">
              <div className="justify flex items-center justify-between gap-2">
                <InputRoot>
                  <InputIcon>
                    <Search />
                  </InputIcon>
                  <Input disabled placeholder="Search" />
                </InputRoot>
                <Button
                  disabled={isRefreshing || isFetchingNavigationTrees || isFetchingWorksheets}
                  onClick={handleRefresh}
                  size="icon"
                  variant="ghost"
                  className="text-muted-foreground size-8"
                >
                  <RefreshCw
                    className={cn(
                      (isRefreshing || isFetchingNavigationTrees || isFetchingWorksheets) &&
                        'animate-spin',
                    )}
                  />
                </Button>
              </div>
            </SidebarGroup>
          )}
          <SidebarGroup className="h-full px-0 pb-0">
            <SidebarGroupContent className="h-full">
              <TabsContent value="worksheets" className="h-full">
                <SqlEditorLeftPanelWorksheets worksheets={worksheets} />
              </TabsContent>

              <TabsContent value="databases" className="h-full">
                <ResizablePanelGroup direction="vertical">
                  <SqlEditorResizablePanel
                    minSize={10}
                    order={1}
                    defaultSize={100}
                    className='overflow-auto"'
                  >
                    <ScrollArea className="h-full">
                      <SqlEditorLeftPanelDatabases
                        navigationTrees={navigationTrees}
                        selectedTree={selectedNavigationTreeDatabase}
                        onSetSelectedTree={(tree: SelectedTree) => {
                          setSelectedNavigationTreeDatabase(tree);
                          if (!isLeftBottomPanelExpanded) {
                            leftBottomRef.current?.resize(20);
                          }
                        }}
                      />
                      <ScrollBar orientation="vertical" />
                    </ScrollArea>
                  </SqlEditorResizablePanel>
                  {selectedNavigationTreeDatabase && (
                    <ResizableHandle withHandle onDragging={setIsResizing} />
                  )}
                  <SqlEditorResizablePanel
                    ref={leftBottomRef}
                    order={2}
                    onCollapse={() => {
                      setLeftBottomPanelExpanded(false);
                    }}
                    onExpand={() => {
                      setLeftBottomPanelExpanded(true);
                    }}
                    collapsible
                    defaultSize={selectedNavigationTreeDatabase ? 25 : 0}
                    minSize={20}
                  >
                    <SqlEditorLeftPanelTableFields selectedTree={selectedNavigationTreeDatabase} />
                  </SqlEditorResizablePanel>
                </ResizablePanelGroup>
              </TabsContent>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
      </Tabs>
    </div>
  );
};
