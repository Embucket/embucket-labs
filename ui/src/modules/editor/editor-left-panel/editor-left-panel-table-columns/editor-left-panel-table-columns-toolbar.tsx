import { Eye, Table, X } from 'lucide-react';

import { Button } from '@/components/ui/button';
import type { SelectedTree } from '@/modules/shared/trees/trees-items';
import type { TablePreviewDataColumn } from '@/orval/models';

import { useEditorPanelsState } from '../../editor-panels-state-provider';

interface EditorLeftPanelTableColumnsToolbarProps {
  selectedTree?: SelectedTree;
  onSetOpen: (opened: boolean) => void;
  previewData: TablePreviewDataColumn[];
}

export const EditorLeftPanelTableColumnsToolbar = ({
  selectedTree,
  onSetOpen,
  previewData,
}: EditorLeftPanelTableColumnsToolbarProps) => {
  const { toggleLeftBottomPanel } = useEditorPanelsState();

  return (
    <div className="mt-4 flex items-center justify-between px-4">
      <div className="flex items-center overflow-hidden py-2 select-none">
        <Table className="size-4 flex-shrink-0" />
        <p className="mx-2 truncate text-sm font-medium">{selectedTree?.tableName}</p>
      </div>

      <div className="flex flex-shrink-0 items-center gap-1">
        {/* <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button size="icon" variant="ghost" className="text-muted-foreground size-8">
              <MoreVertical />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent side="right" align="start">
            <DropdownMenuItem onClick={() => {}}>
              <span>Load data</span>
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => {}}>
              <span>Preview data</span>
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu> */}
        <Button
          onClick={() => onSetOpen(true)}
          size="icon"
          variant="ghost"
          className="text-muted-foreground size-8"
          disabled={!previewData.length}
        >
          <Eye className="size-4" />
        </Button>
        <Button
          onClick={toggleLeftBottomPanel}
          size="icon"
          variant="ghost"
          className="text-muted-foreground size-8"
        >
          <X className="size-4" />
        </Button>
      </div>
    </div>
  );
};
