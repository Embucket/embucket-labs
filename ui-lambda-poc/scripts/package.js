const { buildSync } = require('esbuild');
const { execSync } = require('child_process');
const { rmSync, mkdirSync, cpSync, existsSync } = require('fs');
const { join } = require('path');

console.log('⚡ Packaging for Lambda (POC Mode)...');

const root = process.cwd();
const dist = join(root, 'dist');

// 1. Clean & Prep
if (existsSync(dist)) rmSync(dist, { recursive: true });
mkdirSync(dist);
mkdirSync(join(dist, 'ui')); // Create parent folder structure

// 2. Build UI (Skip if already exists to save time)
const uiSource = join(root, 'ui', 'dist');
if (!existsSync(uiSource)) {
    console.log('Building UI...');
    execSync('cd ui && pnpm install && pnpm build', { stdio: 'inherit' });
}

// 3. Bundle Backend
console.log('Bundling with Esbuild...');
try {
  buildSync({
      entryPoints: ['index.js'],
      bundle: true,
      platform: 'node',
      target: 'node20',
      outfile: join(dist, 'index.js'),
      external: ['pg-native'], // Exclude C++ bindings
      minify: true,
  });
} catch (e) {
  console.error('Build failed:', e);
  process.exit(1);
}

// 4. Copy UI Assets
console.log('Copying UI assets...');
if (existsSync(uiSource)) {
  cpSync(uiSource, join(dist, 'ui', 'dist'), { recursive: true });
} else {
  console.warn('⚠️ UI dist folder not found. Skipping UI copy.');
}

// 5. Zip
console.log('Zipping package...');
try {
  // cd into dist to zip contents without the 'dist' folder itself
  execSync(`cd dist && zip -r -q ../function.zip .`);
  console.log('\n✅ Done! Upload "function.zip" to AWS Lambda.');
} catch (e) {
  console.error('❌ Zip failed. Do you have "zip" installed?', e.message);
}