import { useCallback } from 'react';

import { useEditorCacheContext } from '@tidbcloud/tisqleditor-react';

import { useUpdateWorksheet } from '@/orval/worksheets';

export function useSaveEditorContent() {
  const cacheCtx = useEditorCacheContext();
  const { mutate } = useUpdateWorksheet();

  const save = useCallback(
    (worksheetId: number) => {
      const activeEditor = cacheCtx.getEditor('MyEditor');
      if (!activeEditor) return;

      const content = activeEditor.editorView.state.doc.toString();

      mutate({
        data: { content },
        worksheetId,
      });
    },
    [cacheCtx, mutate],
  );

  return { save };
}
