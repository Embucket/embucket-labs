import { useCallback, useEffect } from 'react';

import type { SqlStatement } from '@tidbcloud/codemirror-extension-sql-parser';
import { useEditorCacheContext } from '@tidbcloud/tisqleditor-react';

import { Button } from '@/components/ui/button';

interface RunSQLButtonProps {
  onRunQuery: (query: string) => void;
  disabled?: boolean;
}

export function EditorCenterPanelToolbarRunSqlButton({ onRunQuery, disabled }: RunSQLButtonProps) {
  const cacheCtx = useEditorCacheContext();

  const handleRunQuery = useCallback(() => {
    const activeEditor = cacheCtx.getEditor('MyEditor');
    if (!activeEditor) return;
    let targetStatement: SqlStatement | undefined;
    const curStatements = activeEditor.getCurStatements();

    if (curStatements[0].content === '') {
      targetStatement = activeEditor.getNearbyStatement();
    } else {
      targetStatement = curStatements.at(-1);
    }
    if (!targetStatement?.content) return;

    onRunQuery(targetStatement.content);
  }, [cacheCtx, onRunQuery]);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.metaKey && event.key === 'Enter') {
        event.preventDefault();
        handleRunQuery();
      }
    }

    window.addEventListener('keydown', handleKeyDown, { capture: true });
    return () => {
      window.removeEventListener('keydown', handleKeyDown, { capture: true });
    };
  }, [handleRunQuery]);

  return (
    <div className="w-fit">
      <Button size="sm" className="justify-start" disabled={disabled} onClick={handleRunQuery}>
        Run <span className="opacity-40">⌘ ⏎</span>
      </Button>
    </div>
  );
}
