#!/usr/bin/env node

import { readFileSync, readdirSync, statSync } from 'fs';
import { join } from 'path';

const CONTENT_DIR = 'src/content/docs';
const REQUIRED_FIELDS = ['title', 'description'];

function validateFrontmatter(filePath) {
  const content = readFileSync(filePath, 'utf-8');
  const lines = content.split('\n');

  // Check if file starts with frontmatter
  if (!lines[0].trim().startsWith('---')) {
    return { valid: false, error: 'Missing frontmatter' };
  }

  // Find end of frontmatter
  let endIndex = -1;
  for (let i = 1; i < lines.length; i++) {
    if (lines[i].trim() === '---') {
      endIndex = i;
      break;
    }
  }

  if (endIndex === -1) {
    return { valid: false, error: 'Incomplete frontmatter' };
  }

  // Extract frontmatter
  const frontmatter = lines.slice(1, endIndex).join('\n');

  // Check for required fields
  for (const field of REQUIRED_FIELDS) {
    if (!frontmatter.includes(`${field}:`)) {
      return { valid: false, error: `Missing required field: ${field}` };
    }
  }

  return { valid: true };
}

function walkDir(dir) {
  const files = [];
  const items = readdirSync(dir);

  for (const item of items) {
    const fullPath = join(dir, item);
    const stat = statSync(fullPath);

    if (stat.isDirectory()) {
      files.push(...walkDir(fullPath));
    } else if (item.endsWith('.md') || item.endsWith('.mdx')) {
      files.push(fullPath);
    }
  }

  return files;
}

// Main validation
const contentFiles = walkDir(CONTENT_DIR);
let hasErrors = false;

console.log('Validating content files...\n');

for (const file of contentFiles) {
  const result = validateFrontmatter(file);
  const relativePath = file.replace(process.cwd() + '/', '');

  if (!result.valid) {
    console.error(`❌ ${relativePath}: ${result.error}`);
    hasErrors = true;
  } else {
    console.log(`✅ ${relativePath}`);
  }
}

if (hasErrors) {
  console.error('\n❌ Content validation failed!');
  process.exit(1);
} else {
  console.log('\n✅ All content files are valid!');
}
