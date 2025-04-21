/** @typedef  {import("@ianvs/prettier-plugin-sort-imports").PluginConfig} SortImportsConfig*/
/** @typedef  {import("prettier").Config} PrettierConfig*/
/** @type { PrettierConfig | SortImportsConfig } */
const config = {
  printWidth: 100, // max line width
  tabWidth: 2, // visual width" of of the "tab"
  semi: true, // add semicolons at the end of statements
  singleQuote: true, // '' for stings instead of ""
  arrowParens: 'always', // braces even for single param in arrow functions (a) => { }
  trailingComma: 'all', // add trailing commas in objects, arrays, etc...
  jsxSingleQuote: false, // "" for react props (like in html)
  plugins: [
    '@ianvs/prettier-plugin-sort-imports',
    'prettier-plugin-packagejson',
    'prettier-plugin-tailwindcss',
  ],
  importOrder: [
    '^(react/(.*)$)|^(react$)',
    '',
    '<THIRD_PARTY_MODULES>',
    '',
    '^@/(.*)$',
    '',
    '^[./]',
  ],
  importOrderParserPlugins: ['typescript', 'jsx', 'decorators-legacy'],
};

export default config;
