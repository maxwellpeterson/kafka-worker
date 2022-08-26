// These functions are only intended to be used in test files!

// Convert an ArrayBuffer to a string that can be snapshotted
// Taken from https://stackoverflow.com/a/9458996
export const base64 = (buffer: ArrayBuffer): string => {
  let binary = "";
  const len = buffer.byteLength;
  const bytes = new Uint8Array(buffer);
  for (let i = 0; i < len; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
};

// Generate an ArrayBuffer of arbitrary bytes for testing
export const fillBuffer = (length: number): ArrayBuffer => {
  const buffer = new ArrayBuffer(length);
  const view = new DataView(buffer);
  for (let i = 0; i < length; i++) {
    // This will wrap around when i > 127 but that's fine
    view.setInt8(i, i);
  }
  return buffer;
};

// I should upgrade to Node 17+ to get structuredClone()
export const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
