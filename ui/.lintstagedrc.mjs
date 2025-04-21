export default {
  '*.{ts,tsx}': () => [
    'tsc -p ./tsconfig.app.json',
    'eslint --fix -c ./eslint.config.mjs --max-warnings 0 --report-unused-disable-directives --ignore-pattern "**/*.{mjs,cjs,js}"',
  ],
  '*': 'prettier --write --ignore-unknown',
};
