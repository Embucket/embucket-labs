import { ResizablePanelGroup } from '@/components/ui/resizable';
import {
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
} from '@/components/ui/sidebar';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useGetWorksheets } from '@/orval/worksheets';

import { useSqlEditorPanelsState } from '../sql-editor-panels-state-provider';
import { SqlEditorResizableHandle, SqlEditorResizablePanel } from '../sql-editor-resizable';
import type { LeftPanelTab } from '../sql-editor-settings-store';
import { useSqlEditorSettingsStore } from '../sql-editor-settings-store';
import { SqlEditorLeftBottomPanel } from './sql-editor-left-panel-table-columns/sql-editor-left-bottom-panel';
import { SqlEditorLeftPanelTrees } from './sql-editor-left-panel-trees/sql-editor-left-panel-trees';
import { SqlEditorLeftPanelWorksheetsToolbar } from './sql-editor-left-panel-worksheets-toolbar';
import { SqlEditorLeftPanelWorksheets } from './sql-editor-left-panel-worksheets/sql-editor-left-panel-worksheets';

export const SqlEditorLeftPanel = () => {
  const selectedTree = useSqlEditorSettingsStore((state) => state.selectedTree);
  const selectedLeftPanelTab = useSqlEditorSettingsStore((state) => state.selectedLeftPanelTab);
  const setSelectedLeftPanelTab = useSqlEditorSettingsStore(
    (state) => state.setSelectedLeftPanelTab,
  );

  const {
    data: { items: worksheets } = {},
    refetch: refetchWorksheets,
    isFetching: isFetchingWorksheets,
  } = useGetWorksheets();

  const { leftBottomRef, setLeftBottomPanelExpanded } = useSqlEditorPanelsState();

  return (
    <>
      <Tabs
        defaultValue="worksheets"
        className="size-full gap-0 text-nowrap"
        value={selectedLeftPanelTab}
        onValueChange={(value) => setSelectedLeftPanelTab(value as LeftPanelTab)}
      >
        {/* Tabs */}
        <SidebarHeader className="p-4 pb-0">
          <TabsList className="w-full min-w-50 text-nowrap">
            <TabsTrigger value="databases">Databases</TabsTrigger>
            <TabsTrigger value="worksheets">Worksheets</TabsTrigger>
          </TabsList>
        </SidebarHeader>

        <SidebarContent className="gap-0 overflow-hidden">
          <SidebarGroup className="h-full p-0">
            <SidebarGroupContent className="h-full">
              {/* Databases */}
              <TabsContent value="databases" className="h-full">
                <ResizablePanelGroup direction="vertical">
                  <SqlEditorResizablePanel minSize={10} order={1} defaultSize={100}>
                    <SqlEditorLeftPanelTrees />
                  </SqlEditorResizablePanel>
                  {selectedTree?.tableName && <SqlEditorResizableHandle />}
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
                <SqlEditorLeftPanelWorksheetsToolbar
                  isFetchingWorksheets={isFetchingWorksheets}
                  onRefetchWorksheets={refetchWorksheets}
                />
                <SqlEditorLeftPanelWorksheets
                  isFetchingWorksheets={isFetchingWorksheets}
                  worksheets={worksheets ?? []}
                />
              </TabsContent>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
      </Tabs>
    </>
  );
};
