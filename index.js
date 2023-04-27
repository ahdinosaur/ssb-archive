import { dirname, join } from 'node:path'
import { writeFile } from 'node:fs/promises'
import got from 'got'
import { mkdirp } from 'mkdirp'
import cheerio from 'cheerio'
import mime from 'mime-types'

export async function archive(options = {}) {
  const { outDir, host } = options

  const visited = new Set()

  await downloadPageRecursively('/profile', { outDir, host, visited })
}

async function downloadPage(url, options) {
  const { outDir, host, visited } = options

  // skip
  if (
    visited.has(url) ||
    !url.startsWith('/') ||
    url === '/theme.css'
  ) return [null, '']

  visited.add(url, true)

  console.log('url', url)
  let doc
  try {
    doc = await got(url.substring(1), { prefixUrl: host })
  } catch (err) {
    console.error(err)
    return [null, '']
  }

  const ext = mime.extension(doc.headers['content-type'])

  let page
  switch (ext) {
    case 'html':
      page = getHtmlPage(doc.body)
      break
    default:
      page = doc.body
      break
  }

  const path = join(
    outDir,
    url.includes('.')
      ? url
      : url + '.' + ext
  )

  await mkdirp(dirname(path))
  await writeFile(path, page)

  return [ext, page]
}

const MAX_DEPTH = 1

async function downloadPageRecursively(url, options, { depth = 0 } = {}) {
  const [ext, page] = await downloadPage(url, options)

  if (ext === 'html') {
    const requisites = getHtmlPageRequisites(page)
    for (const link of requisites) {
      await downloadPage(link, options)
    }

    /*
    if (depth < MAX_DEPTH) {
      const links = getHtmlPageLinks(page)
      for (const link of links) {
        await downloadPageRecursively(link, options, { depth: depth + 1 })
      }
    }
    */
  }
}

function getHtmlPage(doc) {
  const $ = cheerio.load(doc)
  $('body > nav').remove()
  $('form').remove()
  return $.html()
}

function getHtmlPageLinks(doc) {
  const $ = cheerio.load(doc)
  let links = []
  $('a').each((i, elem) => {
    links.push($(elem).attr('href'))
  })
  return links
}

function getHtmlPageRequisites(doc) {
  const $ = cheerio.load(doc)
  let links = []
  $('link').each((i, elem) => {
    links.push($(elem).attr('href'))
  })
  $('img').map((i, elem) => {
    links.push($(elem).attr('src'))
  })
  return links
}

export default archive
