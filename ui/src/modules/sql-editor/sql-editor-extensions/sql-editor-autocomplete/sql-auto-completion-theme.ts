import { EditorView } from '@codemirror/view';

export const baseTheme = EditorView.baseTheme({
  '.cm-tooltip-autocomplete.cm-tooltip': {
    boxShadow: '0px 2px 16px rgba(0, 0, 0, 0.1)',
    border: 'none',
    borderRadius: '8px',
  },
  '.cm-tooltip-autocomplete.cm-tooltip ul': {
    borderRadius: '8px',
  },
  '.cm-tooltip.cm-tooltip-autocomplete .cm-autocomplete-item': {
    lineHeight: '32px',
    padding: '0 16px',
    '&:hover': {
      backgroundColor: `rgba(12, 166, 242, .1)`,
    },
    '&[aria-selected]': {
      background: 'rgba(12, 166, 242, .1)',
      color: '#333333',
    },
  },
  '.cm-autocomplete-item': {
    display: 'flex',
    alignItems: 'center',
  },
  '.cm-autocomplete-item > div.icon': {
    marginRight: '10px',
  },
  '.cm-autocomplete-item > div.icon svg': {
    verticalAlign: 'middle',
  },
  '.cm-autocomplete-item .cm-completionIcon-keyword': {
    paddingRight: '24px',
  },
  '&light .cm-tooltip-autocomplete.cm-tooltip': {
    backgroundColor: '#fff',
  },
  '&light .cm-autocomplete-item': {
    backgroundColor: '#fff',
    color: '#333333',
  },
  '&dark .cm-tooltip-autocomplete.cm-tooltip': {
    backgroundColor: '#111',
  },
  '&dark .cm-tooltip.cm-tooltip-autocomplete .cm-autocomplete-item': {
    backgroundColor: '#111',
    color: '#e8e8e8',

    '&[aria-selected]': {
      background: 'rgba(12, 166, 242, .1)',
      color: '#e8e8e8',
    },
  },
});
