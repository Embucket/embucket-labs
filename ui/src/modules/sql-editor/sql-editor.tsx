import { useMemo } from 'react';

import { type Extension } from '@codemirror/state';
import { EditorView } from '@codemirror/view';
import { useParams } from '@tanstack/react-router';
import { curSqlGutter } from '@tidbcloud/codemirror-extension-cur-sql-gutter';
import { SQLEditor as TiSQLEditor } from '@tidbcloud/tisqleditor-react';

// import type { Worksheet } from '@/orval/models';
import { useGetNavigationTrees } from '@/orval/navigation-trees';
import { useGetWorksheet } from '@/orval/worksheets';

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
  const { worksheetId } = useParams({ from: '/sql-editor/$worksheetId/' });

  const { data: worksheet } = useGetWorksheet(+worksheetId);
  const { data: { items: navigationTrees } = {} } = useGetNavigationTrees();
  // const docChangeHandler = (view: EditorView, state: EditorState, doc: string) => {
  //   console.log(doc);
  // };

  const exts: Extension[] = useMemo(
    () => [
      sqlAutoCompletion(),
      // setDbLinter(),
      setCustomKeymaps(),
      curSqlGutter(),
      EditorView.lineWrapping,
      EditorView.editorAttributes.of({ class: readonly ? 'readonly' : '' }),
      readonly ? EditorView.editable.of(false) : EditorView.editable.of(true),
      // onDocChange(docChangeHandler),
    ],
    [readonly],
  );

  return (
    <TiSQLEditor
      editorId="MySQLEditor"
      doc={content ?? worksheet?.content ?? ''}
      theme={SQL_EDITOR_THEME}
      sqlConfig={{
        upperCaseKeywords: true,
        schema: transformNavigationTreeToSqlConfigSchema(navigationTrees),
      }}
      extraExts={exts}
    />
  );
}
