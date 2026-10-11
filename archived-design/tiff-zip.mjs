// tiff-zip.mjs
//
// Lossless ZIP (Deflate, TIFF compression 8) recompression of an uncompressed
// classic TIFF such as Photoshop writes. Only the main image data is
// recompressed; every other tag is copied byte for byte, including Photoshop's
// layer data (37724), resources (34377), XMP, IPTC, ICC and the Exif/GPS
// directories. Nothing is returned until the result has been read back and
// checked: every strip must inflate to exactly the original image bytes and
// every other tag must be unchanged. Runs in browsers and in Node 18+.
//
//   const { blob, originalSize, size } = await zipTiff(file, (done) => ...);
//
// Throws NotSupported when the file can't be handled safely (not a TIFF,
// BigTIFF, already compressed, tiled, several pages, unknown offset tags...);
// the caller then uploads the original.

const TYPE_SIZE = { 1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 6: 1, 7: 1, 8: 2, 9: 4, 10: 8, 11: 4, 12: 8, 13: 4 };
const COMPRESSION = 259;
const STRIP_OFFSETS = 273;
const ROWS_PER_STRIP = 278;
const STRIP_BYTE_COUNTS = 279;
const REWRITTEN = new Set([COMPRESSION, STRIP_OFFSETS, ROWS_PER_STRIP, STRIP_BYTE_COUNTS]);
// Directories pointed to by a tag; rewritten at their new place.
const SUB_IFDS = new Set([34665, 34853]); // Exif, GPS
// Tags holding file offsets this code doesn't relocate: tiles, SubIFDs, free
// space, old-style JPEG thumbnails, Exif interoperability directory.
const UNSUPPORTED = new Set([288, 289, 322, 323, 324, 325, 330, 513, 514, 40965]);
// Uncompressed size of each new strip. Big strips compress better; ~1 MB keeps
// memory low in readers that inflate a whole strip at once.
const TARGET_STRIP_BYTES = 1 << 20;
// Not worth it if the saving is smaller than this.
const MIN_SAVING = 0.05;

export class NotSupported extends Error {}

const read = async (blob, start, end) => new Uint8Array(await blob.slice(start, end).arrayBuffer());

async function readIfd(blob, offset, le) {
  const n = new DataView((await read(blob, offset, offset + 2)).buffer).getUint16(0, le);
  const dv = new DataView((await read(blob, offset + 2, offset + 2 + n * 12 + 4)).buffer);
  const entries = [];
  for (let i = 0; i < n; i++) {
    const p = i * 12;
    const tag = dv.getUint16(p, le);
    const type = dv.getUint16(p + 2, le);
    const count = dv.getUint32(p + 4, le);
    if (!TYPE_SIZE[type]) throw new NotSupported(`tag ${tag} has unknown type ${type}`);
    entries.push({
      tag, type, count,
      size: TYPE_SIZE[type] * count,
      inline: new Uint8Array(dv.buffer.slice(p + 8, p + 12)),
      offset: dv.getUint32(p + 8, le),
    });
  }
  return { entries, next: dv.getUint32(n * 12, le) };
}

// A tag's value bytes, wherever they are stored.
const valueBytes = (blob, e) =>
  e.size <= 4 ? Promise.resolve(e.inline.slice(0, e.size)) : read(blob, e.offset, e.offset + e.size);

// SHORT/LONG values as numbers.
async function numbers(blob, e, le) {
  const dv = new DataView((await valueBytes(blob, e)).buffer);
  const step = TYPE_SIZE[e.type];
  if (step !== 2 && step !== 4) throw new NotSupported(`tag ${e.tag} is not SHORT/LONG`);
  return Array.from({ length: e.count }, (_, i) => (step === 2 ? dv.getUint16(i * 2, le) : dv.getUint32(i * 4, le)));
}

const deflate = (blob) => new Response(blob.stream().pipeThrough(new CompressionStream('deflate'))).blob();
const inflate = async (blob) =>
  new Uint8Array(await new Response(blob.stream().pipeThrough(new DecompressionStream('deflate'))).arrayBuffer());

function sameBytes(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

// Byte ranges [start, end) of the new strips in the original file.
async function newStripRanges(file, ifd, le) {
  const get = (tag) => ifd.entries.find((e) => e.tag === tag);
  const offsets = await numbers(file, get(STRIP_OFFSETS), le);
  const counts = await numbers(file, get(STRIP_BYTE_COUNTS), le);
  if (offsets.length !== counts.length) throw new NotSupported('strip offsets and counts differ in number');
  const keep = { ranges: offsets.map((o, i) => [o, o + counts[i]]), rowsPerStrip: null };

  // Re-strip only the simple, common case: one contiguous run of chunky rows.
  const planar = get(284) ? (await numbers(file, get(284), le))[0] : 1;
  const contiguous = offsets.every((o, i) => i === 0 || offsets[i - 1] + counts[i - 1] === o);
  if (planar !== 1 || !contiguous) return keep;
  const width = (await numbers(file, get(256), le))[0];
  const height = (await numbers(file, get(257), le))[0];
  const spp = get(277) ? (await numbers(file, get(277), le))[0] : 1;
  const bps = get(258) ? await numbers(file, get(258), le) : [1];
  const bitsPerPixel = bps.length === spp ? bps.reduce((a, b) => a + b, 0) : bps[0] * spp;
  const rowBytes = Math.ceil((width * bitsPerPixel) / 8);
  const total = counts.reduce((a, b) => a + b, 0);
  if (rowBytes * height !== total) return keep;

  const rows = Math.max(1, Math.floor(TARGET_STRIP_BYTES / rowBytes));
  const ranges = [];
  for (let r = 0; r < height; r += rows) {
    ranges.push([offsets[0] + r * rowBytes, offsets[0] + Math.min(height, r + rows) * rowBytes]);
  }
  return { ranges, rowsPerStrip: rows };
}

export async function zipTiff(file, onProgress = () => {}) {
  const head = await read(file, 0, 8);
  const hv = new DataView(head.buffer);
  const order = hv.getUint16(0);
  const le = order === 0x4949;
  if (!le && order !== 0x4d4d) throw new NotSupported('not a TIFF');
  if (hv.getUint16(2, le) !== 42) throw new NotSupported('BigTIFF or not a TIFF');

  const ifd = await readIfd(file, hv.getUint32(4, le), le);
  if (ifd.next !== 0) throw new NotSupported('more than one page');
  for (const e of ifd.entries) {
    if (UNSUPPORTED.has(e.tag) || (e.type === 13 && !SUB_IFDS.has(e.tag))) throw new NotSupported(`tag ${e.tag} not handled`);
  }
  const compression = ifd.entries.find((e) => e.tag === COMPRESSION);
  const current = compression ? (await numbers(file, compression, le))[0] : 1;
  if (current !== 1) throw new NotSupported(`already compressed (${current})`);
  if (!ifd.entries.some((e) => e.tag === STRIP_OFFSETS)) throw new NotSupported('no strips');

  const subIfds = new Map();
  for (const e of ifd.entries.filter((x) => SUB_IFDS.has(x.tag))) {
    const sub = await readIfd(file, (await numbers(file, e, le))[0], le);
    if (sub.next !== 0) throw new NotSupported(`directory ${e.tag} is chained`);
    for (const s of sub.entries) {
      if (UNSUPPORTED.has(s.tag) || s.type === 13) throw new NotSupported(`tag ${s.tag} in ${e.tag} not handled`);
    }
    subIfds.set(e.tag, sub);
  }

  // 1. Compress.
  const { ranges, rowsPerStrip } = await newStripRanges(file, ifd, le);
  const total = ranges.reduce((a, [s, t]) => a + (t - s), 0);
  const strips = [];
  let done = 0;
  for (const [start, end] of ranges) {
    strips.push(await deflate(file.slice(start, end)));
    done += end - start;
    onProgress(0.5 * (done / total));
  }

  // 2. Lay out the new file: header, IFD, values, sub-IFDs, strip tables, strips.
  const parts = [];
  let pos = 0;
  const put = (part, length) => {
    if (pos % 2) { parts.push(new Uint8Array(1)); pos += 1; } // values start on a word boundary
    const at = pos;
    parts.push(part);
    pos += length;
    return at;
  };
  const u16 = (v) => { const b = new Uint8Array(2); new DataView(b.buffer).setUint16(0, v, le); return b; };
  const u32 = (v) => { const b = new Uint8Array(4); new DataView(b.buffer).setUint32(0, v, le); return b; };
  const longs = (vals) => { const b = new Uint8Array(vals.length * 4); const dv = new DataView(b.buffer); vals.forEach((v, i) => dv.setUint32(i * 4, v, le)); return b; };

  const ifdSize = (n) => 2 + n * 12 + 4;
  const ifdBuf = new Uint8Array(ifdSize(ifd.entries.length));
  head.set(u32(8), 4); // the directory follows the 8-byte header
  put(head, 8);
  put(ifdBuf, ifdBuf.length);

  // Copies a directory's out-of-line values and returns its entries' new 4-byte fields.
  const placeValues = async (entries, skip) => {
    const fields = new Map();
    for (const e of entries) {
      if (skip.has(e.tag)) continue;
      if (e.size <= 4) fields.set(e.tag, e.inline);
      else fields.set(e.tag, u32(put(file.slice(e.offset, e.offset + e.size), e.size)));
    }
    return fields;
  };
  const writeIfd = (buf, entries, fields, overrides) => {
    const dv = new DataView(buf.buffer);
    dv.setUint16(0, entries.length, le);
    entries.forEach((e, i) => {
      const o = overrides.get(e.tag) || { type: e.type, count: e.count, field: fields.get(e.tag) };
      const p = 2 + i * 12;
      dv.setUint16(p, e.tag, le);
      dv.setUint16(p + 2, o.type, le);
      dv.setUint32(p + 4, o.count, le);
      buf.set(o.field, p + 8);
    });
    dv.setUint32(2 + entries.length * 12, 0, le);
  };

  const fields = await placeValues(ifd.entries, new Set([...REWRITTEN, ...SUB_IFDS]));
  const overrides = new Map();
  for (const [tag, sub] of subIfds) {
    const subFields = await placeValues(sub.entries, new Set());
    const buf = new Uint8Array(ifdSize(sub.entries.length));
    writeIfd(buf, sub.entries, subFields, new Map());
    const original = ifd.entries.find((e) => e.tag === tag);
    overrides.set(tag, { type: original.type, count: 1, field: u32(put(buf, buf.length)) });
  }

  const sizes = strips.map((s) => s.size);
  const countsField = sizes.length === 1 ? u32(sizes[0]) : u32(put(longs(sizes), sizes.length * 4));
  const offsetsTable = new Uint8Array(sizes.length * 4);
  const offsetsField = sizes.length === 1 ? null : u32(put(offsetsTable, offsetsTable.length));
  const stripAt = strips.map((s) => put(s, s.size));
  stripAt.forEach((at, i) => offsetsTable.set(u32(at), i * 4));

  const short = (v) => { const b = new Uint8Array(4); b.set(u16(v)); return b; };
  overrides.set(COMPRESSION, { type: 3, count: 1, field: short(8) });
  overrides.set(STRIP_OFFSETS, { type: 4, count: sizes.length, field: offsetsField || u32(stripAt[0]) });
  overrides.set(STRIP_BYTE_COUNTS, { type: 4, count: sizes.length, field: countsField });
  if (rowsPerStrip !== null) overrides.set(ROWS_PER_STRIP, { type: 4, count: 1, field: u32(rowsPerStrip) });
  else {
    const e = ifd.entries.find((x) => x.tag === ROWS_PER_STRIP);
    if (e) overrides.set(ROWS_PER_STRIP, { type: e.type, count: e.count, field: e.inline });
  }
  writeIfd(ifdBuf, ifd.entries, fields, overrides);

  const blob = new Blob(parts, { type: 'image/tiff' });
  if (blob.size > file.size * (1 - MIN_SAVING)) throw new NotSupported('saves too little');

  // 3. Check the result by reading it back like a TIFF reader would.
  await verify(file, ifd, subIfds, ranges, blob, le, (f) => onProgress(0.5 + 0.5 * f));
  return { blob, originalSize: file.size, size: blob.size, strips: strips.length };
}

async function verify(file, ifd, subIfds, ranges, blob, le, onProgress) {
  const nh = new DataView((await read(blob, 0, 8)).buffer);
  const out = await readIfd(blob, nh.getUint32(4, le), le);
  const fail = (why) => { throw new Error(`compressed copy failed its check: ${why}`); };
  if (out.next !== 0 || out.entries.length !== ifd.entries.length) fail('directory shape');

  const sameTags = async (a, b, skip) => {
    for (let i = 0; i < a.length; i++) {
      const x = a[i], y = b[i];
      if (x.tag !== y.tag) fail(`tag order at ${x.tag}`);
      if (skip.has(x.tag)) continue;
      if (x.type !== y.type || x.count !== y.count) fail(`tag ${x.tag} type/count`);
      if (!sameBytes(await valueBytes(file, x), await valueBytes(blob, y))) fail(`tag ${x.tag} value`);
    }
  };
  await sameTags(ifd.entries, out.entries, new Set([...REWRITTEN, ...SUB_IFDS]));
  for (const [tag, sub] of subIfds) {
    const e = out.entries.find((x) => x.tag === tag);
    const newSub = await readIfd(blob, (await numbers(blob, e, le))[0], le);
    if (newSub.entries.length !== sub.entries.length) fail(`directory ${tag} shape`);
    for (let i = 0; i < sub.entries.length; i++) {
      const x = sub.entries[i], y = newSub.entries[i];
      if (x.tag !== y.tag || x.type !== y.type || x.count !== y.count) fail(`tag ${x.tag} in ${tag}`);
      if (!sameBytes(await valueBytes(file, x), await valueBytes(blob, y))) fail(`tag ${x.tag} value in ${tag}`);
    }
  }

  const get = (tag) => out.entries.find((e) => e.tag === tag);
  if ((await numbers(blob, get(COMPRESSION), le))[0] !== 8) fail('compression tag');
  const offsets = await numbers(blob, get(STRIP_OFFSETS), le);
  const counts = await numbers(blob, get(STRIP_BYTE_COUNTS), le);
  if (offsets.length !== ranges.length) fail('strip count');
  const total = ranges.reduce((a, [s, t]) => a + (t - s), 0);
  let done = 0;
  for (let i = 0; i < offsets.length; i++) {
    const [start, end] = ranges[i];
    const back = await inflate(blob.slice(offsets[i], offsets[i] + counts[i]));
    if (!sameBytes(back, await read(file, start, end))) fail(`image data in strip ${i}`);
    done += end - start;
    onProgress(done / total);
  }
}
