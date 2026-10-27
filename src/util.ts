import crypto from 'node:crypto';

export default function id(urlsafe = true): string {
  const randStr = crypto.randomBytes(8).toString('base64');
  return urlsafe ? randStr.replaceAll('+', '-').replaceAll('/', '_') : randStr;
}
