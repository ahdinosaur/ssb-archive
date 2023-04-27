#!/usr/bin/env node

import meow from 'meow'

import archive from './index.js'

const cli = meow(`
  Usage
    $ ssb-archive

  Options
    --outDir, -o Output directory
    --host Host server (Oasis)

  Examples
    $ ssb-archive -o output
`, {
  importMeta: import.meta,
  flags: {
    outDir: {
      type: 'string',
      alias: 'o',
      isRequired: true,
    },
    host: {
      type: 'string',
      default: 'http://localhost:3000',
    }
  }
})

archive(cli.flags)
