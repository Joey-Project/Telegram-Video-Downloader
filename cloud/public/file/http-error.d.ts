export class HttpResponseError extends Error {
  readonly status: number;
  constructor(status: number, message: string);
}
