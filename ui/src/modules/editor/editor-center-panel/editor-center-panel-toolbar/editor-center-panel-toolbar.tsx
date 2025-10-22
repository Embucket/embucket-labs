import { SidebarGroup } from '@/components/ui/sidebar';

import { EditorContextDropdown } from '../editor-context-dropdown/editor-context-dropdown';
import { EditorCenterPanelToolbarBeautifyButton } from './editor-center-panel-toolbar-beautify-button';
import { EditorCenterPanelToolbarRunSqlButton } from './editor-center-panel-toolbar-run-sql-button';
import { EditorCenterPanelToolbarShareButton } from './editor-center-panel-toolbar-share-button';

interface EditorToolbarProps {
  onRunQuery: (query: string) => void;
  isLoading: boolean;
}

export const EditorCenterPanelToolbar = ({ onRunQuery, isLoading }: EditorToolbarProps) => {
  return (
    <div>
      <SidebarGroup className="flex justify-between border-b p-4">
        <div className="flex items-center gap-2">
          <EditorCenterPanelToolbarRunSqlButton onRunQuery={onRunQuery} disabled={isLoading} />
          <EditorContextDropdown />
          <div className="ml-auto flex items-center gap-1">
            <EditorCenterPanelToolbarBeautifyButton />
            <EditorCenterPanelToolbarShareButton />
          </div>
        </div>
      </SidebarGroup>
    </div>
  );
};
