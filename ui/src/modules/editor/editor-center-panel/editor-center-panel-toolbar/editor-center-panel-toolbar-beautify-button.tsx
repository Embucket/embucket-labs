import { useEditorCacheContext } from '@tidbcloud/tisqleditor-react';
import { Wand2 } from 'lucide-react';
import { format } from 'sql-formatter';

import { Button } from '@/components/ui/button';

export const EditorCenterPanelToolbarBeautifyButton = () => {
  const cacheCtx = useEditorCacheContext();

  const handleBeautify = () => {
    const activeEditor = cacheCtx.getEditor('MyEditor');
    if (!activeEditor) return;

    const fullContent = activeEditor.editorView.state.doc.toString();
    const beautified = format(fullContent);

    activeEditor.editorView.dispatch({
      changes: { from: 0, to: activeEditor.editorView.state.doc.length, insert: beautified },
      selection: { anchor: activeEditor.editorView.state.doc.length },
    });
  };

  return (
    <Button
      onClick={handleBeautify}
      size="icon"
      variant="ghost"
      className="text-muted-foreground size-8"
    >
      <Wand2 />
    </Button>
  );
};
