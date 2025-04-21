import { PanelBottom, PanelLeft, PanelRight, PanelTop, Rows2 } from 'lucide-react';

import { Toggle } from '@/components/ui/toggle';

import { useSqlEditorPanelsState } from './sql-editor-panels-state-provider';

export const SqlEditorFooter = () => {
  const {
    isTopPanelExpanded,
    isBottomPanelExpanded,
    isLeftPanelExpanded,
    isRightPanelExpanded,
    toggleLeftPanel,
    toggleRightPanel,
    toggleBottomPanel,
    toggleTopPanel,
    alignPanels,
  } = useSqlEditorPanelsState();

  return (
    <div className="border-t p-2">
      <div className="text-muted-foreground flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Toggle size="sm" onClick={toggleLeftPanel} pressed={isLeftPanelExpanded}>
            <PanelLeft className="size-4" />
          </Toggle>
        </div>
        <div className="flex items-center gap-1">
          <Toggle size="sm" onClick={toggleBottomPanel} pressed={!isBottomPanelExpanded}>
            <PanelBottom className="size-4" />
          </Toggle>
          <Toggle
            size="sm"
            onClick={alignPanels}
            pressed={isBottomPanelExpanded && isTopPanelExpanded}
          >
            <Rows2 className="size-4" />
          </Toggle>
          <Toggle size="sm" onClick={toggleTopPanel} pressed={!isTopPanelExpanded}>
            <PanelTop className="size-4" />
          </Toggle>
        </div>
        <Toggle size="sm" pressed={isRightPanelExpanded} onPressedChange={toggleRightPanel}>
          <PanelRight className="size-4" />
        </Toggle>
      </div>
    </div>
  );
};
