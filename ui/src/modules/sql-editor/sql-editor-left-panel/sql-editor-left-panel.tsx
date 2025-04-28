import { ResizablePanelGroup } from '@/components/ui/resizable';
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
import { SqlEditorResizableHandle, SqlEditorResizablePanel } from '../sql-editor-resizable';
import type { LeftPanelTab } from '../sql-editor-settings-store';
import { useSqlEditorSettingsStore } from '../sql-editor-settings-store';
import { useSqlEditorTabsSync } from '../use-sql-editor-tabs-sync';
import { SqlEditorLeftPanelDatabasesToolbar } from './sql-editor-left-panel-databases-toolbar';
import { SqlEditorLeftBottomPanel } from './sql-editor-left-panel-table-columns/sql-editor-left-bottom-panel';
import { SqlEditorLeftPanelTrees } from './sql-editor-left-panel-trees/sql-editor-left-panel-trees';
import { SqlEditorLeftPanelWorksheetsToolbar } from './sql-editor-left-panel-worksheets-toolbar';
import { SqlEditorLeftPanelWorksheets } from './sql-editor-left-panel-worksheets/sql-editor-left-panel-worksheets';

export const SqlEditorLeftPanel = () => {
  const selectedTree = useSqlEditorSettingsStore((state) => state.selectedTree);
  const leftPanelTab = useSqlEditorSettingsStore((state) => state.leftPanelTab);
  const setLeftPanelTab = useSqlEditorSettingsStore((state) => state.setLeftPanelTab);

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

  const { leftBottomRef, setLeftBottomPanelExpanded } = useSqlEditorPanelsState();

  useSqlEditorTabsSync();

  return (
    <>
      <Tabs
        defaultValue="worksheets"
        className="size-full gap-0 text-nowrap"
        value={leftPanelTab}
        onValueChange={(value) => setLeftPanelTab(value as LeftPanelTab)}
      >
        {/* Tabs */}
        <SidebarHeader className="h-[60px] p-4 pb-2">
          <TabsList className="w-full min-w-50 text-nowrap">
            <TabsTrigger value="databases">Databases</TabsTrigger>
            <TabsTrigger value="worksheets">Worksheets</TabsTrigger>
          </TabsList>
        </SidebarHeader>

        <SidebarContent className="gap-0 overflow-hidden">
          {/* Databases Toolbar */}
          <TabsContent value="databases" className="h-full">
            <SqlEditorLeftPanelDatabasesToolbar
              isFetchingNavigationTrees={isFetchingNavigationTrees}
              onRefetchNavigationTrees={refetchNavigationTrees}
            />
          </TabsContent>
          {/* Worksheets Toolbar */}
          <TabsContent value="worksheets" className="h-full">
            <SqlEditorLeftPanelWorksheetsToolbar
              isFetchingWorksheets={isFetchingWorksheets}
              onRefetchWorksheets={refetchWorksheets}
            />
          </TabsContent>
          <SidebarGroup className="h-full p-0">
            <SidebarGroupContent className="h-full">
              {/* Databases */}
              <TabsContent value="databases" className="h-full">
                <ResizablePanelGroup direction="vertical">
                  <SqlEditorResizablePanel minSize={10} order={1} defaultSize={100}>
                    <SqlEditorLeftPanelTrees
                      navigationTrees={navigationTrees ?? []}
                      isFetchingNavigationTrees={isFetchingNavigationTrees}
                    />
                  </SqlEditorResizablePanel>
                  {selectedTree && <SqlEditorResizableHandle />}
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
                    defaultSize={selectedTree ? 25 : 0}
                    minSize={20}
                  >
                    <SqlEditorLeftBottomPanel />
                  </SqlEditorResizablePanel>
                </ResizablePanelGroup>
              </TabsContent>
              {/* Worksheets */}
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
