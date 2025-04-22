export default {
  '*.{ts,tsx}': () => [
    'cd ui && tsc -p ./tsconfig.app.json',
    'cd ui &&  eslint --fix -c ./eslint.config.mjs --max-warnings 0 --report-unused-disable-directives --ignore-pattern "**/*.{mjs,cjs,js}"',
  ],
  '*': 'cd ui && prettier --write --ignore-unknown',
};
