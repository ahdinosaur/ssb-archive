import { sep, dirname, join } from 'node:path'
import { writeFile } from 'node:fs/promises'
import got from 'got'
import { mkdirp } from 'mkdirp'
import cheerio from 'cheerio'
import Mime from 'mime-types'
import PQueue from 'p-queue'
import QuickLRU from 'quick-lru'

export async function archive(options = {}) {
  const { outDir, host } = options

  const userIds = [
    '@6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519',
    '@SKIc/gO6Du10rTHqujjAP1+LHzVP441U2YsQeU+Up84=.ed25519',
    '@DUuG1ivTjZqtPYlDXHukLqsCDvjq5lkYmuZo+N4Oqc8=.ed25519',
  ]
  
  const queue = new PQueue({
    concurrency: 16,
  })
  const down = new QuickLRU({
    maxSize: 1000
  })
  const done = new Set()
  const urls = new Map()

  for (const userId of userIds) {
    const authorUrl = `/author/${encodeURIComponent(userId)}`
    await getDoc(authorUrl, { outDir, host, down, done, urls, userIds, queue })
  }

  await queue.onIdle()

  const indexPage = `
    <html>
      <body>
        <ul>
          ${userIds.map(userId => (
            `<li>
              <a href="/author/${encodeURIComponent(userId)}.html">
                ${userId}
              </a>
            </li>`
          )).join('\n')}
        </ul>
      </body>
    </html>
  `
  await writeFile(join(outDir, 'index.html'), indexPage)
}

const processors = {
  html: processHtml,
}

async function getDoc(url, options, { depth = 0 } = {}) {
  const { done } = options


  // skip
  if (
    !url.startsWith('/') ||
    url === '/theme.css'
  ) return

  const [urlWithoutHash] = url.split('#')
  if (done.has(urlWithoutHash)) return
  done.add(urlWithoutHash)

  let doc = await downloadDoc(url, options)
  if (doc == null) return
  const { filePath, ext } = await processUrl(url, options)
  const processor = processors[ext]
  if (processor != null) {
    doc = await processor(doc.body, options, { depth })
  } else {
    doc = doc.rawBody
  }

  await mkdirp(dirname(filePath))
  await writeFile(filePath, doc)
}

const MAX_DEPTH = Infinity

async function processHtml(doc, options, { depth }) {
  const { queue } = options

  const $ = cheerio.load(doc)

  // remove things
  $('body > nav').remove()
  $('span:contains("This is you")').remove()
  $('span:contains("You are following")').remove()
  $('a:contains("Edit profile")').remove()
  $('form:contains("Unfollow")').remove()

  // transform post footer
  $('section.post > footer > div').each((i, el) => {
    const children = $(el).children()
    const likes = children.first()
    const json = children.last()

    $(el).replaceWith(
      $(`<div>
          <form action="javascript:void(0);">
            ${likes.find('button').prop('outerHTML')}
          </form>
          ${json.prop('outerHTML')}
        </div>`)
    )
  })

  // get page links
  for (const link of $('link')) {
    const url = $(link).attr('href')
    queue.add(() => getDoc(url, options, { depth: depth + 1 }))
  }

  // get page images
  for (const img of $('img')) {
    const url = $(img).attr('src')
    queue.add(() => getDoc(url, options, { depth: depth + 1 }))
    const { url: newUrl } = await processUrl(url, options)
    $(img).attr('src', newUrl)
  }

  // get page images
  for (const img of $('img')) {
    const url = $(img).attr('src')
    queue.add(() => getDoc(url, options, { depth: depth + 1 }))
    const { url: newUrl } = await processUrl(url, options)
    $(img).attr('src', newUrl)
  }

  /*
  if (isPriorityProfile($, options)) {
    for (const link of $('a:contains("Older posts")')) {
      const url = $(link).attr('href')
      queue.add(() => getDoc(url, options, { depth }))
      const { url: newUrl } = await processUrl(url, options)
      $(link).attr('href', newUrl)
    }
  }
  */

  for (const link of $('a')) {
    // if ($(link).text() === "Older posts") continue
    const url = $(link).attr('href')
    if (!url.startsWith('/')) continue
    if (depth < MAX_DEPTH) {
      queue.add(() => getDoc(url, options, { depth: depth + 1 }))
    }
    const { url: newUrl } = await processUrl(url, options)
    $(link).attr('href', newUrl)
  }

  return $.html()
}

function isPriorityProfile($, options) {
  const { userIds } = options
  const profileId = $('pre.md-mention > .hljs-link').text()
  return userIds.includes(profileId)
}

async function processUrl(url, options) {
  const { outDir, urls, userIds } = options

  if (url == null) return null
  if (!url.startsWith('/')) return { url }
  if (urls.has(url)) return urls.get(url)

  let urlPath, urlHash
  if (url.includes('#')) {
    const splits = url.split('#')
    urlPath = splits[0]
    urlHash = `#${splits[1]}`
  } else {
    urlPath = url
    urlHash = ''
  }

  if (urlPath.startsWith('/profile')) {
    urlPath = urlPath.replace('/profile', `/author/${encodeURIComponent(userIds[0])}`)
  }

  let ext
  if (urlPath.startsWith('/json')) {
    urlPath = urlPath.replace('/json', '/message')
    ext = 'json'
  } else if (
    urlPath.startsWith('/author') ||
    urlPath.startsWith('/thread') ||
    urlPath.startsWith('/hashtag') ||
    urlPath.startsWith('/likes') ||
    urlPath.startsWith('/subtopic')
  ) {
    ext = 'html'
  } else if (urlPath.startsWith('/image')) {
    ext = 'png'
  }
  if (ext == null) {
    const doc = await downloadDoc(url, options)
    if (doc != null && doc.headers['content-type']) {
      ext = Mime.extension(doc.headers['content-type'])
    }
  }

  if (urlPath.includes('?')) {
    const [filePath, params] = urlPath.split('?')
    if (filePath.endsWith('/')) {
      urlPath = `${filePath}${params}`
    } else {
      urlPath = `${filePath}/${params}`
    }
  }

  let filePath = decodeURIComponent(urlPath)
  if (ext != null && !filePath.endsWith('.' + ext)) {
    urlPath = urlPath + '.' + ext
    filePath = filePath + '.' + ext
  }
  filePath = join(outDir, filePath)

  const result = {
    url: urlPath + urlHash,
    ext,
    filePath,
  }
  urls.set(url, result)
  return result
}

async function downloadDoc(url, options) {
  const { down, host } = options

  let [urlWithoutHash, hash] = url.split('#')

  if (down.has(urlWithoutHash)) return down.get(urlWithoutHash)

  console.log('download', urlWithoutHash)

  let doc
  try {
    doc = await got(urlWithoutHash.substring(1), {
      prefixUrl: host,
      retry: {
        limit: 4,
        backoffLimit: 5000,
      }
    })
  } catch (err) {
    console.error(err)
  }

  down.set(urlWithoutHash, doc)

  return doc
}

export default archive
