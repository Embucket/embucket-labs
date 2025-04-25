import { createFileRoute } from '@tanstack/react-router';

import { SqlEditorPage } from '@/modules/sql-editor/sql-editor-page';
import { SqlEditorPanelsStateProvider } from '@/modules/sql-editor/sql-editor-panels-state-provider';

export const Route = createFileRoute('/sql-editor/$worksheetId/')({
  component: () => (
    <SqlEditorPanelsStateProvider>
      <SqlEditorPage />
    </SqlEditorPanelsStateProvider>
  ),
});
