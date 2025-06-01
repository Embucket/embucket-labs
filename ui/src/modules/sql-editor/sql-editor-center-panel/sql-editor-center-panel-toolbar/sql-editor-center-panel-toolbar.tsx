import { SidebarGroup } from '@/components/ui/sidebar';

import { SqlEditorContextDropdown } from '../sql-editor-context-dropdown/sql-editor-context-dropdown';
import { SqlEditorCenterPanelToolbarBeautifyButton } from './sql-editor-center-panel-toolbar-beautify-button';
import { SqlEditorCenterPanelToolbarRunSqlButton } from './sql-editor-center-panel-toolbar-run-sql-button';
import { SqlEditorCenterPanelToolbarShareButton } from './sql-editor-center-panel-toolbar-share-button';

interface SqlEditorToolbarProps {
  onRunQuery: (query: string) => void;
}

export const SqlEditorCenterPanelToolbar = ({ onRunQuery }: SqlEditorToolbarProps) => {
  return (
    <div>
      <SidebarGroup className="flex justify-between border-b p-4">
        <div className="flex items-center gap-2">
          <SqlEditorCenterPanelToolbarRunSqlButton onRunQuery={onRunQuery} />
          <SqlEditorContextDropdown />
          <div className="ml-auto flex items-center gap-1">
            <SqlEditorCenterPanelToolbarBeautifyButton />
            <SqlEditorCenterPanelToolbarShareButton />
          </div>
        </div>
      </SidebarGroup>
    </div>
  );
};
