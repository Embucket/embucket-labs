import { HighlightStyle, syntaxHighlighting } from '@codemirror/language';
import type { Extension } from '@codemirror/state';
import { EditorView } from '@codemirror/view';
import { tags as t } from '@lezer/highlight';

// Cope from https://github.com/codemirror/theme-one-dark/blob/main/src/one-dark.ts as ref

// The editor theme styles for One Dark.
const theme = EditorView.theme(
  {
    // Root styles
    '&': {
      backgroundColor: 'var(--background)',
      fontSize: '13px',
      paddingLeft: '16px',
    },
    // Editor content
    '.cm-content': {
      padding: '16px 8px',
    },
    // Cursor
    '.cm-cursor, .cm-dropCursor': {
      borderLeftColor: 'var(--color-foreground)',
    },
    // Selection
    '&.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground, .cm-selectionBackground, .cm-content ::selection':
      { backgroundColor: 'var(--color-gray-700)' },
    // Active line
    '.cm-activeLine': { backgroundColor: 'var(--color-transparent)' },
    // Selection match
    '.cm-selectionMatch': { backgroundColor: 'var(--color-gray-700)' },
    // Matching brackets
    '&.cm-focused .cm-matchingBracket, &.cm-focused .cm-nonmatchingBracket': {
      backgroundColor: 'var(--color-gray-700)',
    },
    // Gutter
    '.cm-gutters': {
      backgroundColor: 'var(--background)',
      color: 'var(--muted-foreground)',
      border: 'none',
    },
    '&.readonly .cm-gutters': {
      display: 'none !important',
    },
    '&.readonly': {
      padding: '0px !important',
      background: 'transparent !important',
    },
    '&.readonly .cm-content': {
      padding: '0px !important',
    },
    '&.readonly .cm-line': {
      padding: '0px !important',
    },
    '.cm-gutterElement': {
      padding: '0px !important',
      textAlign: 'left !important',
    },
    '.cm-activeLineGutter': { backgroundColor: 'transparent', color: 'var(--color-foreground)' },
    '.cm-lineNumbers': { minWidth: '24px' },
    '.cm-line': { lineHeight: '20px !important' },
    // Tooltip
    '.cm-tooltip-autocomplete': {
      background: 'var(--background) !important',
      border: '1px solid var(--border) !important',
      padding: '4px !important',
      borderRadius: 'var(--radius) !important',
    },
    '.cm-autocomplete-item': {
      display: 'flex !important',
      alignItems: 'center !important',
      height: '32px !important',
      padding: '4px 8px !important',
    },
    '.cm-autocomplete-item[aria-selected]': {
      padding: '4px 8px !important',
      background: 'var(--hover) !important',
      borderRadius: '4px !important',
    },
    '.cm-completionIcon': {
      display: 'none !important',
    },
    '.cm-completionLabel': {
      margin: '0px 8px !important',
    },
    '.cm-autocomplete-item .type': {
      marginLeft: 'auto !important',
      color: 'var(--muted-foreground) !important',
    },
  },
  { dark: true },
);

const chalky = '#e5c07b',
  coral = '#e06c75',
  cyan = '#56b6c2',
  invalid = '#ffffff',
  ivory = '#abb2bf',
  stone = '#7d8799', // Brightened compared to original to increase contrast
  malibu = '#61afef',
  sage = '#98c379',
  whiskey = '#d19a66',
  violet = '#c678dd';

const oneDarkHighlightStyle = HighlightStyle.define([
  { tag: t.keyword, color: violet },
  { tag: [t.name, t.deleted, t.character, t.propertyName, t.macroName], color: chalky },
  { tag: [t.function(t.variableName), t.labelName], color: malibu },
  { tag: [t.color, t.constant(t.name), t.standard(t.name)], color: whiskey },
  { tag: [t.definition(t.name), t.separator], color: ivory },
  {
    tag: [
      t.typeName,
      t.className,
      t.number,
      t.changed,
      t.annotation,
      t.modifier,
      t.self,
      t.namespace,
    ],
    color: chalky,
  },
  {
    tag: [t.operator, t.operatorKeyword, t.url, t.escape, t.regexp, t.link, t.special(t.string)],
    color: cyan,
  },
  { tag: [t.meta, t.comment], color: stone },
  { tag: t.strong, fontWeight: 'bold' },
  { tag: t.emphasis, fontStyle: 'italic' },
  { tag: t.strikethrough, textDecoration: 'line-through' },
  { tag: t.link, color: stone, textDecoration: 'underline' },
  { tag: t.heading, fontWeight: 'bold', color: coral },
  { tag: [t.atom, t.bool, t.special(t.variableName)], color: whiskey },
  { tag: [t.processingInstruction, t.string, t.inserted], color: sage },
  { tag: t.invalid, color: invalid },
]);

// Extension to enable the One Dark theme (both the editor theme and the highlight style).
export const SQL_EDITOR_THEME: Extension = [theme, syntaxHighlighting(oneDarkHighlightStyle)];
