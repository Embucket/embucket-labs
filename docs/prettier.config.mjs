/** @typedef  {import("prettier").Config} PrettierConfig*/
/** @type { PrettierConfig } */
const config = {
  printWidth: 100, // max line width
  tabWidth: 2, // visual width" of of the "tab"
  semi: true, // add semicolons at the end of statements
  singleQuote: true, // '' for stings instead of ""
  arrowParens: 'always', // braces even for single param in arrow functions (a) => { }
  trailingComma: 'all', // add trailing commas in objects, arrays, etc...
  jsxSingleQuote: false, // "" for react props (like in html)
  plugins: [
    'prettier-plugin-astro',
    'prettier-plugin-packagejson',
    // needs to be last
    'prettier-plugin-tailwindcss',
  ],
};

export default config;
