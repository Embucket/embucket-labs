import { useCallback, useEffect, useMemo, useRef } from 'react';

import { Compartment, type Extension } from '@codemirror/state';
import { EditorView } from '@codemirror/view';
import { useQueryClient } from '@tanstack/react-query';
import { useParams } from '@tanstack/react-router';
import { curSqlGutter } from '@tidbcloud/codemirror-extension-cur-sql-gutter';
import { saveHelper } from '@tidbcloud/codemirror-extension-save-helper';
import { SQLEditor, useEditorCacheContext } from '@tidbcloud/tisqleditor-react';

import { useGetNavigationTrees } from '@/orval/navigation-trees';
import { getGetWorksheetQueryKey, useGetWorksheet, useUpdateWorksheet } from '@/orval/worksheets';

import { sqlAutoCompletion } from './editor-extensions/editor-autocomplete/auto-completion';
import { setCustomKeymaps } from './editor-extensions/editor-custom-keymaps';
import { SQL_EDITOR_THEME } from './editor-theme';
import { transformNavigationTreeToSqlConfigSchema } from './editor-utils';

// Create compartment outside component to persist across renders
const saveHelperCompartment = new Compartment();

interface EditorProps {
  readonly?: boolean;
  content?: string;
}

// This hook now only depends on the content string and checks
// if an update is necessary before dispatching.
const useSetWorksheetContent = (worksheetContent?: string) => {
  const cacheCtx = useEditorCacheContext();

  useEffect(() => {
    // Wait for content to be defined/loaded
    if (!worksheetContent) return;

    const activeEditor = cacheCtx.getEditor('MyEditor');
    if (!activeEditor) return;

    // Get the editor's current content
    const currentDoc = activeEditor.editorView.state.doc.toString();

    // Only dispatch a change if the new content is actually different
    // from the content already in the editor.
    if (worksheetContent !== currentDoc) {
      activeEditor.editorView.dispatch({
        changes: {
          from: 0,
          to: activeEditor.editorView.state.doc.length,
          insert: worksheetContent,
        },
      });
    }
  }, [worksheetContent, cacheCtx]); // Depend on the primitive string
};

export function Editor({ readonly, content }: EditorProps) {
  const queryClient = useQueryClient();
  const cacheCtx = useEditorCacheContext();

  const worksheetId = useParams({
    from: '/sql-editor/$worksheetId/',
    select: (params) => params.worksheetId,
    shouldThrow: false,
  });

  const { data: worksheet } = useGetWorksheet(Number(worksheetId));
  const { data: { items: navigationTrees } = {} } = useGetNavigationTrees();
  const { mutate } = useUpdateWorksheet();

  // Pass only the worksheet content string to the updated hook
  useSetWorksheetContent(worksheet?.content);

  // Invalidate queries when worksheet ID changes
  useEffect(() => {
    queryClient.invalidateQueries({
      queryKey: getGetWorksheetQueryKey(Number(worksheetId)),
    });
  }, [worksheetId, queryClient]);

  // Use ref to store the latest values without triggering re-renders
  const latestValuesRef = useRef({
    worksheetName: worksheet?.name,
    worksheetId: Number(worksheetId),
  });

  // Update ref when worksheet changes
  useEffect(() => {
    latestValuesRef.current = {
      worksheetName: worksheet?.name,
      worksheetId: Number(worksheetId),
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

  const exts: Extension[] = useMemo(
    () => [
      sqlAutoCompletion(),
      setCustomKeymaps(),
      curSqlGutter(),
      // include compartment so it can be reconfigured when handleSave changes
      saveHelperCompartment.of([]),
      EditorView.lineWrapping,
      EditorView.editorAttributes.of({ class: readonly ? 'readonly' : '' }),
      readonly ? EditorView.editable.of(false) : EditorView.editable.of(true),
    ],
    [readonly],
  );

  // Use effect to update the saveHelper compartment when handleSave changes
  useEffect(() => {
    const activeEditor = cacheCtx.getEditor('MyEditor');
    activeEditor?.editorView.dispatch({
      effects: saveHelperCompartment.reconfigure(
        saveHelper({
          save: handleSave,
          delay: 1000,
        }),
      ),
    });
  }, [handleSave, cacheCtx]);

  // `doc` is now for *initial* content only.
  // `useSetWorksheetContent` will handle loading the fetched content.
  const editorDoc = content ?? '';

  // Memoize the schema to prevent it from being recalculated on every render
  const schema = useMemo(
    () => transformNavigationTreeToSqlConfigSchema(navigationTrees),
    [navigationTrees],
  );

  // Memoize the sqlConfig object to prevent it from being a new object
  // on every render, which would re-trigger extensions.
  const sqlConfig = useMemo(
    () => ({
      upperCaseKeywords: true,
      schema: schema,
    }),
    [schema],
  );

  return (
    <SQLEditor
      editorId="MyEditor"
      doc={editorDoc}
      theme={SQL_EDITOR_THEME}
      sqlConfig={sqlConfig}
      extraExts={exts}
    />
  );
}
