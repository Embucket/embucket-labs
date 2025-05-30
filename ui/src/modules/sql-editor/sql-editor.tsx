import { useCallback, useEffect, useMemo, useRef } from 'react';

import { Compartment, type Extension } from '@codemirror/state';
import { EditorView } from '@codemirror/view';
import { useQueryClient } from '@tanstack/react-query';
import { useParams } from '@tanstack/react-router';
import { curSqlGutter } from '@tidbcloud/codemirror-extension-cur-sql-gutter';
import { saveHelper } from '@tidbcloud/codemirror-extension-save-helper';
import { SQLEditor as TiSQLEditor, useEditorCacheContext } from '@tidbcloud/tisqleditor-react';

import type { Worksheet } from '@/orval/models';
import { useGetNavigationTrees } from '@/orval/navigation-trees';
import { getGetWorksheetQueryKey, useGetWorksheet, useUpdateWorksheet } from '@/orval/worksheets';

import { sqlAutoCompletion } from './sql-editor-extensions/sql-editor-autocomplete/sql-auto-completion';
import { setCustomKeymaps } from './sql-editor-extensions/sql-editor-custom-keymaps';
import { SQL_EDITOR_THEME } from './sql-editor-theme';
import { transformNavigationTreeToSqlConfigSchema } from './sql-editor-utils';

// Create compartment outside component to persist across renders
const saveHelperCompartment = new Compartment();

interface SQLEditorProps {
  readonly?: boolean;
  content?: string;
}

const useSetWorksheetContent = (worksheet?: Worksheet) => {
  const cacheCtx = useEditorCacheContext();

  useEffect(() => {
    if (!worksheet) return;
    const activeEditor = cacheCtx.getEditor('MySQLEditor');
    if (!activeEditor) return;
    activeEditor.editorView.dispatch({
      changes: {
        from: 0,
        to: activeEditor.editorView.state.doc.length,
        insert: worksheet.content,
      },
    });
  }, [worksheet, cacheCtx]);
};

export function SQLEditor({ readonly, content }: SQLEditorProps) {
  const queryClient = useQueryClient();

  const worksheetId = useParams({
    from: '/sql-editor/$worksheetId/',
    select: (params) => params.worksheetId,
  });

  const { data: worksheet } = useGetWorksheet(+worksheetId);
  // Not intended to be used for SQLEditor - there should be a dedicated endpoint for that
  const { data: { items: navigationTrees } = {} } = useGetNavigationTrees();
  const { mutate } = useUpdateWorksheet();

  // Set initial content from fetched worksheet
  useSetWorksheetContent(worksheet);

  // Invalidate queries when worksheet ID changes
  useEffect(() => {
    queryClient.invalidateQueries({
      queryKey: getGetWorksheetQueryKey(+worksheetId),
    });
  }, [worksheetId, queryClient]);

  // Use ref to store the latest values without triggering re-renders
  const latestValuesRef = useRef({
    worksheetName: worksheet?.name,
    worksheetId: +worksheetId,
  });

  // Update ref when worksheet changes
  useEffect(() => {
    latestValuesRef.current = {
      worksheetName: worksheet?.name,
      worksheetId: +worksheetId,
    };
  }, [worksheet?.name, worksheetId]);

  const handleSave = useCallback(
    (view: EditorView) => {
      const { worksheetName, worksheetId: currentWorksheetId } = latestValuesRef.current;
      mutate({
        data: {
          content: view.state.doc.toString(),
          name: worksheetName,
        },
        worksheetId: currentWorksheetId,
      });
    },
    [mutate],
  );

  // TODO: Use to enable / disable Run button
  // @tidbcloud/codemirror-extension-events
  // const handleDocChange = (view: EditorView, state: EditorState, doc: string) => {
  //   console.log(doc, worksheetId);
  // };

  const exts: Extension[] = useMemo(
    () => [
      sqlAutoCompletion(),
      setCustomKeymaps(),
      curSqlGutter(),
      EditorView.lineWrapping,
      EditorView.editorAttributes.of({ class: readonly ? 'readonly' : '' }),
      readonly ? EditorView.editable.of(false) : EditorView.editable.of(true),
      saveHelperCompartment.of(
        saveHelper({
          save: handleSave,
          delay: 1000,
        }),
      ),
    ],
    [readonly, handleSave],
  );

  const editorDoc = content ?? worksheet?.content ?? '';

  return (
    <TiSQLEditor
      editorId="MySQLEditor"
      doc={editorDoc}
      theme={SQL_EDITOR_THEME}
      sqlConfig={{
        upperCaseKeywords: true,
        schema: transformNavigationTreeToSqlConfigSchema(navigationTrees),
      }}
      extraExts={exts}
    />
  );
}
