import { startCompletion } from '@codemirror/autocomplete';
import { Prec } from '@codemirror/state';
import type { EditorView } from '@codemirror/view';
import { keymap } from '@codemirror/view';

export const setCustomKeymaps = () => {
  return Prec.highest(
    keymap.of([
      {
        key: 'Mod-i',
        run: (view: EditorView) => {
          return startCompletion(view);
        },
      },
    ]),
  );
};
