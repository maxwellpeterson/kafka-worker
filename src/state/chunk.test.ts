import { base64, fillMessageSet } from "src/protocol/test-utils";
import { Chunk, prepareMessageSet } from "src/state/chunk";

describe("prepareMessageSet", () => {
  type TestCase = [string, number, Chunk];
  const cases: TestCase[] = [
    [
      "one message",
      1,
      {
        offsetStart: BigInt(0),
        buffer: new ArrayBuffer(256),
        frames: [],
        nextIndex: 0,
      },
    ],
    [
      "one message with existing message",
      1,
      {
        offsetStart: BigInt(0),
        buffer: new ArrayBuffer(256),
        frames: [[0, 56]],
        nextIndex: 56,
      },
    ],
    [
      "muliple messages",
      5,
      {
        offsetStart: BigInt(0),
        buffer: new ArrayBuffer(1024),
        frames: [],
        nextIndex: 0,
      },
    ],
    [
      "muliple messages with existing messages",
      5,
      {
        offsetStart: BigInt(0),
        buffer: new ArrayBuffer(1024),
        frames: [
          [0, 96],
          [96, 64],
        ],
        nextIndex: 160,
      },
    ],
  ];

  test.each(cases)("one chunk: %s", (_name, numMessages, chunk) => {
    const initialChunk = structuredClone(chunk) as Chunk;
    const filler = prepareMessageSet(
      fillMessageSet(numMessages),
      initialChunk.offsetStart
    );

    const messagesFilled = filler.fillChunk(chunk);
    expect(messagesFilled).toEqual(numMessages);

    // Check that existing chunk contents are not modified
    expect(chunk.offsetStart).toEqual(initialChunk.offsetStart);
    expect(chunk.buffer.slice(0, initialChunk.nextIndex)).toEqual(
      initialChunk.buffer.slice(0, initialChunk.nextIndex)
    );
    expect(chunk.frames.slice(0, initialChunk.frames.length)).toEqual(
      initialChunk.frames.slice(0, initialChunk.frames.length)
    );

    // Check invariant that chunk.nextIndex equals end of chunk contents
    const finalFrame = chunk.frames.at(-1);
    if (finalFrame) {
      const [start, size] = finalFrame;
      expect(chunk.nextIndex).toEqual(start + size);
    } else {
      // Chunk has no contents
      expect(chunk.nextIndex).toEqual(0);
    }

    expect(base64(chunk.buffer)).toMatchSnapshot();
    expect(chunk).toMatchSnapshot();
  });

  // test("one message", () => {
  //   const filler = prepareMessageSet(fillMessageSet(1), BigInt(0));
  //   const chunk: Chunk = {
  //     offsetStart: BigInt(0),
  //     buffer: new ArrayBuffer(256),
  //     frames: [],
  //     nextIndex: 0,
  //   };

  //   const messageCount = filler.fillChunk(chunk);
  //   expect(messageCount).toEqual(1);

  //   expect(base64(chunk.buffer)).toMatchSnapshot();
  //   expect(chunk).toMatchInlineSnapshot(`
  //     Object {
  //       "buffer": ArrayBuffer [],
  //       "frames": Array [
  //         Array [
  //           0,
  //           79,
  //         ],
  //       ],
  //       "nextIndex": 79,
  //       "offsetStart": 0n,
  //     }
  //   `);
  // });

  // test("multiple messages", () => {
  //   const filler = prepareMessageSet(fillMessageSet(5), BigInt(0));
  //   const chunk: Chunk = {
  //     offsetStart: BigInt(0),
  //     buffer: new ArrayBuffer(1024),
  //     frames: [],
  //     nextIndex: 0,
  //   };

  //   const messageCount = filler.fillChunk(chunk);
  //   expect(messageCount).toEqual(5);

  //   expect(base64(chunk.buffer)).toMatchSnapshot();
  //   expect(chunk).toMatchInlineSnapshot(`
  //     Object {
  //       "buffer": ArrayBuffer [],
  //       "frames": Array [
  //         Array [
  //           0,
  //           79,
  //         ],
  //         Array [
  //           79,
  //           171,
  //         ],
  //         Array [
  //           250,
  //           257,
  //         ],
  //         Array [
  //           507,
  //           154,
  //         ],
  //         Array [
  //           661,
  //           150,
  //         ],
  //       ],
  //       "nextIndex": 811,
  //       "offsetStart": 0n,
  //     }
  //   `);
  // });
});
