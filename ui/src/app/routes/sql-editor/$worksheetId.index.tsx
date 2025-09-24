import { createFileRoute } from '@tanstack/react-router';

import { EditorPage } from '@/modules/editor/editor-page';
import { EditorPanelsStateProvider } from '@/modules/editor/editor-panels-state-provider';

export const Route = createFileRoute('/sql-editor/$worksheetId/')({
  component: () => (
    <EditorPanelsStateProvider>
      <EditorPage />
    </EditorPanelsStateProvider>
  ),
});
