import { Eye, Table, X } from 'lucide-react';

import { Button } from '@/components/ui/button';

import { useSqlEditorPanelsState } from '../../sql-editor-panels-state-provider';
import type { SelectedTree } from '../sql-editor-left-panel-trees/sql-editor-left-panel-trees-items';

interface SqlEditorLeftPanelTableColumnsToolbarProps {
  selectedTree?: SelectedTree;
  onSetOpen: (opened: boolean) => void;
}

export const SqlEditorLeftPanelTableColumnsToolbar = ({
  selectedTree,
  onSetOpen,
}: SqlEditorLeftPanelTableColumnsToolbarProps) => {
  const { toggleLeftBottomPanel } = useSqlEditorPanelsState();

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
