import { sep, dirname, join } from 'node:path'
import { writeFile } from 'node:fs/promises'
import got from 'got'
import { mkdirp } from 'mkdirp'
import cheerio from 'cheerio'
import Mime from 'mime-types'
import { fileTypeFromBuffer } from 'file-type'

export async function archive(options = {}) {
  const { outDir, host } = options

  const userIds = [
    '@6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519',
    '@SKIc/gO6Du10rTHqujjAP1+LHzVP441U2YsQeU+Up84=.ed25519',
    '@DUuG1ivTjZqtPYlDXHukLqsCDvjq5lkYmuZo+N4Oqc8=.ed25519',
  ]

  const visited = new Map()

  for (const userId of userIds) {
    const authorUrl = `/author/${encodeURIComponent(userId)}`
    await downloadDoc(authorUrl, { outDir, host, visited, userIds })
  }

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

async function downloadDoc(url, options, { depth = 0 } = {}) {
  const { outDir, host, visited, userIds } = options

  if (url == null) return null
  url = url.split('#')[0]

  // skip
  if (
    !url.startsWith('/') ||
    url === '/theme.css'
  ) return url
  if (visited.has(url)) return visited.get(url)

  const origUrl = url

  if (url.startsWith('/profile')) {
    url = url.replace('/profile', `/author/${encodeURIComponent(userIds[0])}`)
  }

  let doc
  try {
    doc = await got(url.substring(1), { prefixUrl: host })
  } catch (err) {
    console.error(err)
    return '/404'
  }

  let ext
  if (doc.headers['content-type']) {
    ext = Mime.extension(doc.headers['content-type'])
  } else {
    const fileType = await fileTypeFromBuffer(doc.rawBody)
    if (fileType != null) {
      ext = fileType.ext
    }
  }

  if (url.includes('?')) {
    const [path, params] = url.split('?')
    if (path.endsWith('/')) {
      url = `${path}${params}`
    } else {
      url = `${path}/${params}`
    }
  }
  let path = decodeURIComponent(url)
  if (ext != null && !path.endsWith('.' + ext)) {
    url = url + '.' + ext
    path = path + '.' + ext
  }
  path = join(outDir, path)

  visited.set(origUrl, url)

  const processor = processors[ext]
  if (processor != null) {
    doc = await processor(doc.body, options, { depth })
  } else {
    doc = doc.rawBody
  }

  await mkdirp(dirname(path))
  await writeFile(path, doc)

  console.log(url)
  return url
}

const MAX_DEPTH = 1

async function processHtml(doc, options, { depth }) {
  const { visited } = options

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
    await downloadDoc(url, options, { depth: depth + 1 })
  }

  // get page images
  for (const img of $('img')) {
    const url = $(img).attr('src')
    const newUrl = await downloadDoc(url, options, { depth: depth + 1 })
    $(img).attr('src', newUrl)
  }

  if (isPriorityProfile($, options)) {
    for (const link of $('a:contains("Older posts")')) {
      const url = $(link).attr('href')
      const newUrl = await downloadDoc(url, options, { depth })
      $(link).attr('href', newUrl)
    }
  }

  if (depth < MAX_DEPTH) {
    for (const link of $('a')) {
      const url = $(link).attr('href')
      const newUrl = await downloadDoc(url, options, { depth: depth + 1 })
      $(link).attr('href', newUrl)
    }
  } else {
    for (const link of $('a')) {
      let url = $(link).attr('href')
      url = url.split('#')[0]
      $(link).attr('href', visited.get(url) || '/404')
    }
  }

  return $.html()
}

function isPriorityProfile($, options) {
  const { userIds } = options
  const profileId = $('pre.md-mention > .hljs-link').text()
  return userIds.includes(profileId)
}

export default archive
