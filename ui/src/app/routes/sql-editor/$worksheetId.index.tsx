import { createFileRoute } from '@tanstack/react-router';

import { SqlEditorPage } from '@/modules/sql-editor/sql-editor-page';

export const Route = createFileRoute('/sql-editor/$worksheetId/')({
  component: SqlEditorPage,
});
