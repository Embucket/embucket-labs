import { useState } from 'react';

import { ResizableHandle, ResizablePanelGroup } from '@/components/ui/resizable';
import {
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
} from '@/components/ui/sidebar';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useGetNavigationTrees } from '@/orval/navigation-trees';
import { useGetWorksheets } from '@/orval/worksheets';

import { useSqlEditorPanelsState } from '../sql-editor-panels-state-provider';
import { SqlEditorResizablePanel } from '../sql-editor-resizable';
import { SqlEditorLeftPanelDatabasesToolbar } from './sql-editor-left-panel-databases-toolbar';
import { SqlEditorLeftPanelTableColumns } from './sql-editor-left-panel-table-columns/sql-editor-left-panel-table-columns';
import { SqlEditorLeftPanelTrees } from './sql-editor-left-panel-trees/sql-editor-left-panel-trees';
import type { SelectedTree } from './sql-editor-left-panel-trees/sql-editor-left-panel-trees-items';
import { SqlEditorLeftPanelWorksheetsToolbar } from './sql-editor-left-panel-worksheets-toolbar';
import { SqlEditorLeftPanelWorksheets } from './sql-editor-left-panel-worksheets/sql-editor-left-panel-worksheets';

export const SqlEditorLeftPanel = () => {
  const {
    data: { items: navigationTrees } = {},
    refetch: refetchNavigationTrees,
    isFetching: isFetchingNavigationTrees,
  } = useGetNavigationTrees();
  const {
    data: { items: worksheets } = {},
    refetch: refetchWorksheets,
    isFetching: isFetchingWorksheets,
  } = useGetWorksheets();

  const [selectedNavigationTreeDatabase, setSelectedNavigationTreeDatabase] =
    useState<SelectedTree>();

  const { leftBottomRef, setIsResizing, setLeftBottomPanelExpanded, isLeftBottomPanelExpanded } =
    useSqlEditorPanelsState();

  return (
    <>
      <Tabs defaultValue="databases" className="size-full gap-0 text-nowrap">
        <SidebarHeader className="h-[60px] p-4 pb-2">
          <TabsList className="w-full text-nowrap">
            <TabsTrigger value="databases">Databases</TabsTrigger>
            <TabsTrigger value="worksheets">Worksheets</TabsTrigger>
          </TabsList>
        </SidebarHeader>

        <SidebarContent className="gap-0 overflow-hidden">
          <SidebarGroup className="h-full p-0">
            <TabsContent value="databases">
              <SqlEditorLeftPanelDatabasesToolbar
                isFetchingNavigationTrees={isFetchingNavigationTrees}
                onRefetchNavigationTrees={refetchNavigationTrees}
              />
            </TabsContent>
            <TabsContent value="worksheets">
              <SqlEditorLeftPanelWorksheetsToolbar
                isFetchingWorksheets={isFetchingWorksheets}
                onRefetchWorksheets={refetchWorksheets}
              />
            </TabsContent>
            <SidebarGroupContent className="h-full">
              <TabsContent value="databases" className="h-full">
                <ResizablePanelGroup direction="vertical">
                  <SqlEditorResizablePanel minSize={10} order={1} defaultSize={100}>
                    <SqlEditorLeftPanelTrees
                      navigationTrees={navigationTrees ?? []}
                      isFetchingNavigationTrees={isFetchingNavigationTrees}
                      selectedTree={selectedNavigationTreeDatabase}
                      onSetSelectedTree={(tree: SelectedTree) => {
                        setSelectedNavigationTreeDatabase(tree);
                        if (!isLeftBottomPanelExpanded) {
                          leftBottomRef.current?.resize(20);
                        }
                      }}
                    />
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
                    <SqlEditorLeftPanelTableColumns selectedTree={selectedNavigationTreeDatabase} />
                  </SqlEditorResizablePanel>
                </ResizablePanelGroup>
              </TabsContent>

              <TabsContent value="worksheets" className="h-full">
                <SqlEditorLeftPanelWorksheets worksheets={worksheets ?? []} />
              </TabsContent>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
      </Tabs>
    </>
  );
};
