import { Env } from "src/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

export type EntrypointFn<Output> = (
  env: Env,
  version: number,
  decoder: Decoder,
  encoder: Encoder
) => Promise<Output>;

export type CheckVersionFn = (version: number) => void;
export type DecodeFn<Req> = (decoder: Decoder) => Req;
export type HandleFn<Req, Res> = (env: Env, request: Req) => Promise<Res>;
export type EncodeFn<Res, Output> = (encoder: Encoder, response: Res) => Output;

export const generateEntrypointFn =
  <Req, Res, Output>(
    apiName: string,
    checkVersion: CheckVersionFn,
    decode: DecodeFn<Req>,
    handle: HandleFn<Req, Res>,
    encode: EncodeFn<Res, Output>
  ): EntrypointFn<Output> =>
  async (
    env: Env,
    version: number,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<Output> => {
    checkVersion(version);

    const request = decode(decoder);
    console.log(
      `Received ${apiName} request: ${JSON.stringify(request, null, 2)}`
    );

    const response = await handle(env, request);
    console.log(
      `Sending ${apiName} response: ${JSON.stringify(response, null, 2)}`
    );

    return encode(encoder, response);
  };

export const generateCheckVersionFn =
  (apiName: string, expected: number): CheckVersionFn =>
  (version: number) => {
    if (version !== expected) {
      throw new Error(
        `Unsupported version of ${apiName} api: expected ${expected} but got ${version}`
      );
    }
  };
