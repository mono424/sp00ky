import { writeFile } from 'fs/promises';
import { dump } from 'js-yaml';
import { app } from './server';

async function generate() {
  const doc = app.getOpenAPI31Document({
    openapi: '3.1.0',
    info: {
      version: '1.0.0',
      title: 'example',
    },
  });

  // Stripping functions and other non-JSON values
  const pureDoc = JSON.parse(JSON.stringify(doc));
  const yamlString = dump(pureDoc);
  await writeFile('openapi.yml', yamlString);
  console.log('openapi.yml generated!');
}

generate();
