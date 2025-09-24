import { ResizablePanelGroup } from '@/components/ui/resizable';
import {
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
} from '@/components/ui/sidebar';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useGetWorksheets } from '@/orval/worksheets';

import { useEditorPanelsState } from '../editor-panels-state-provider';
import { EditorResizableHandle, EditorResizablePanel } from '../editor-resizable';
import type { LeftPanelTab } from '../editor-settings-store';
import { useEditorSettingsStore } from '../editor-settings-store';
import { EditorLeftBottomPanel } from './editor-left-panel-table-columns/editor-left-bottom-panel';
import { EditorLeftPanelTrees } from './editor-left-panel-trees/editor-left-panel-trees';
import { EditorLeftPanelWorksheetsToolbar } from './editor-left-panel-worksheets-toolbar';
import { EditorLeftPanelWorksheets } from './editor-left-panel-worksheets/editor-left-panel-worksheets';

export const EditorLeftPanel = () => {
  const selectedTree = useEditorSettingsStore((state) => state.selectedTree);
  const selectedLeftPanelTab = useEditorSettingsStore((state) => state.selectedLeftPanelTab);
  const setSelectedLeftPanelTab = useEditorSettingsStore((state) => state.setSelectedLeftPanelTab);

  const {
    data: { items: worksheets } = {},
    refetch: refetchWorksheets,
    isFetching: isFetchingWorksheets,
  } = useGetWorksheets();

  const { leftBottomRef, setLeftBottomPanelExpanded } = useEditorPanelsState();

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
                  <EditorResizablePanel minSize={10} order={1} defaultSize={100}>
                    <EditorLeftPanelTrees />
                  </EditorResizablePanel>
                  {selectedTree?.tableName && <EditorResizableHandle />}
                  <EditorResizablePanel
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
                    <EditorLeftBottomPanel />
                  </EditorResizablePanel>
                </ResizablePanelGroup>
              </TabsContent>
              {/* Worksheets */}
              <TabsContent value="worksheets" className="h-full">
                <EditorLeftPanelWorksheetsToolbar
                  isFetchingWorksheets={isFetchingWorksheets}
                  onRefetchWorksheets={refetchWorksheets}
                />
                <EditorLeftPanelWorksheets
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
