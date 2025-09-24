import type { Completion } from '@codemirror/autocomplete';
import { acceptCompletion, autocompletion, completionStatus } from '@codemirror/autocomplete';
import { indentLess, indentMore } from '@codemirror/commands';
import type { EditorState, Extension } from '@codemirror/state';
import type { EditorView } from '@codemirror/view';
import { keymap } from '@codemirror/view';

import { SQL_EDITOR_THEME } from '../../editor-theme';
import type { AutoCompletionConfig } from './auto-completion-types';
import {
  Brackets,
  Calendar,
  Database,
  Functions,
  Keyword,
  Numberic,
  Schema,
  String,
  Table,
  Types,
  Variable,
  Vector,
} from './icons-svg';

const customAutoCompletion = (config: AutoCompletionConfig) => {
  const { maxRenderedOptions, ...rest } = config;
  return autocompletion({
    activateOnTyping: true,
    closeOnBlur: false,
    optionClass: () => `cm-autocomplete-item ${config.autocompleteItemClassName ?? ''}`,
    maxRenderedOptions: maxRenderedOptions ?? 50,
    // override: [
    //   async (context) => {
    //     const langCompletion = context.state.languageDataAt<CompletionSource>(
    //       'autocomplete',
    //       context.pos,
    //     );
    //     const results = await Promise.all(langCompletion.map((source) => source(context)));
    //     const options = results.flatMap((r) => r?.options ?? []);

    //     const match = context.matchBefore(/\bSELECT\s+([\s\S]*?)(?=\s+FROM\b)/i); // Match content strictly between SELECT and FROM

    //     if (match) {
    //       // Modify "selected" after selection
    //       const allOptions = [
    //         { label: 'column1', type: 'variable' },
    //         { label: 'column2', type: 'variable' },
    //       ];

    //       return {
    //         from: match.to,
    //         options: allOptions,
    //       };
    //     }

    //     if (!options.length) {
    //       return null;
    //     }

    //     return {
    //       from: context.pos,
    //       options,
    //     };
    //   },
    // ],
    addToOptions: [
      {
        render: (completion: Completion, _state: EditorState, _view: EditorView) => {
          let src = '';
          switch ((completion.type ?? '').toUpperCase()) {
            case 'KEYWORD':
              src = Keyword;
              break;
            case 'VARIABLE':
              src = Variable;
              break;
            case 'SCHEMA':
              src = Schema;
              break;
            case 'TYPE':
              src = Types;
              break;
            case 'FUNCTION':
            case 'METHOD':
              src = Functions;
              break;
            case 'TABLE':
              src = Table;
              break;
            case 'DATABASE':
              src = Database;
              break;
            case 'BIT':
            case 'BOOLEAN':
            case 'TINYINT':
            case 'SMALLINT':
            case 'MEDIUMINT':
            case 'INTEGER':
            case 'INT':
            case 'BIGINT':
            case 'FLOAT':
            case 'DOUBLE':
            case 'DECIMAL':
            case 'NUMERIC':
              src = Numberic;
              break;
            case 'CHAR':
            case 'VARCHAR':
            case 'TEXT':
            case 'TINYTEXT':
            case 'MEDIUMTEXT':
            case 'LONGTEXT':
            case 'BINARY':
            case 'VARBINARY':
            case 'BLOB':
            case 'TINYBLOB':
            case 'MEDIUMBLOB':
            case 'LONGBLOB':
            case 'ENUM':
            case 'SET':
              src = String;
              break;
            case 'DATE':
            case 'TIME':
            case 'DATETIME':
            case 'TIMESTAMP':
            case 'YEAR':
              src = Calendar;
              break;
            case 'JSON':
              src = Brackets;
              break;
            case 'VECTOR<FLOAT>':
            case 'VECTOR<FLOAT16>':
            case 'VECTOR':
              src = Vector;
              break;
          }

          if (completion.type && config.renderIconMap?.[completion.type]) {
            src = config.renderIconMap[completion.type];
          }

          if (src) {
            const element = document.createElement('div');
            element.className = 'icon';
            element.innerHTML = src;
            return element;
          } else {
            return null;
          }
        },
        position: 0,
      },
      {
        render: (completion: Completion, _state: EditorState, _view: EditorView) => {
          const element = document.createElement('div');
          element.className = 'type';
          element.innerHTML = completion.type ?? '';
          return element;
        },
        position: 100,
      },
    ],
    ...rest,
  });
};

const autoCompleteTab = (config: AutoCompletionConfig) => {
  return keymap.of([
    {
      key: config.acceptKey ?? 'Tab',
      preventDefault: true,
      shift: indentLess,
      run: (e) => {
        if (!completionStatus(e.state)) return indentMore(e);
        return acceptCompletion(e);
      },
    },
  ]);
};

export function sqlAutoCompletion(config: AutoCompletionConfig = {}): Extension {
  return [SQL_EDITOR_THEME, customAutoCompletion(config), autoCompleteTab(config)];
}
