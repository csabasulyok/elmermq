import crypto from 'crypto';

export default function id(urlsafe = true): string {
  const randStr = crypto.randomBytes(8).toString('base64');
  return urlsafe ? randStr.replace(/\+/g, '-').replace(/\//g, '_') : randStr;
}
