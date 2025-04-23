import { useEffect, useMemo } from 'react';

import { type Extension } from '@codemirror/state';
import { EditorView } from '@codemirror/view';
import { useParams } from '@tanstack/react-router';
import { curSqlGutter } from '@tidbcloud/codemirror-extension-cur-sql-gutter';
import { saveHelper } from '@tidbcloud/codemirror-extension-save-helper';
import { SQLEditor as TiSQLEditor, useEditorCacheContext } from '@tidbcloud/tisqleditor-react';

// import type { Worksheet } from '@/orval/models';
import { useGetNavigationTrees } from '@/orval/navigation-trees';
import { useGetWorksheet, useUpdateWorksheet } from '@/orval/worksheets';

import { sqlAutoCompletion } from './sql-editor-extensions/sql-editor-autocomplete/sql-auto-completion';
import { setCustomKeymaps } from './sql-editor-extensions/sql-editor-custom-keymaps';
import { transformNavigationTreeToSqlConfigSchema } from './sql-editor-schema';
import { SQL_EDITOR_THEME } from './sql-editor-theme';

// const CONTENT = `-- Example:

// SELECT * FROM mydb.myschema.mytable;`;

// const DATA: Worksheet = {
//   id: 1,
//   content: CONTENT,
//   createdAt: '2023-10-01T12:00:00Z',
//   name: 'Users',
//   updatedAt: '2023-10-01T12:00:05Z',
// };

interface SQLEditorProps {
  readonly?: boolean;
  content?: string;
}

export function SQLEditor({ readonly, content }: SQLEditorProps) {
  const cacheCtx = useEditorCacheContext();

  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });

  const { data: worksheet } = useGetWorksheet(+worksheetId);
  // Not intended to be used for SQLEditor - there should be a dedicated endpoint for that
  const { data: { items: navigationTrees } = {} } = useGetNavigationTrees();
  const { mutate } = useUpdateWorksheet();

  useEffect(() => {
    const activeEditor = cacheCtx.getEditor('MySQLEditor');
    if (!activeEditor) return;
    activeEditor.editorView.dispatch({
      changes: {
        from: 0,
        to: activeEditor.editorView.state.doc.length,
        insert: worksheet?.content,
      },
    });
  }, [worksheet, cacheCtx]);

  // TODO: Use to enable / disable Run button
  // @tidbcloud/codemirror-extension-events
  // const docChangeHandler = (view: EditorView, state: EditorState, doc: string) => {
  //   console.log(doc);
  // };

  const exts: Extension[] = useMemo(
    () => [
      sqlAutoCompletion(),
      setCustomKeymaps(),
      curSqlGutter(),
      EditorView.lineWrapping,
      EditorView.editorAttributes.of({ class: readonly ? 'readonly' : '' }),
      readonly ? EditorView.editable.of(false) : EditorView.editable.of(true),
      saveHelper({
        save: (view: EditorView) => {
          mutate({
            data: {
              content: view.state.doc.toString(),
              name: worksheet?.name,
            },
            worksheetId: +worksheetId,
          });
        },
      }),
    ],
    [readonly, worksheet, mutate, worksheetId],
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
