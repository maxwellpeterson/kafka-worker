// These functions are only intended to be used in test files!

export const mockEnv = () => ({
  HOSTNAME: "example.com",
  PORT: "443",
  PARTITION_CHUNK_SIZE: "4096",
  SESSION: mockDONamespace(),
  CLUSTER: mockDONamespace(),
  PARTITION: mockDONamespace(),
});

const mockDONamespace = () => ({
  newUniqueId: jest.fn<
    ReturnType<DurableObjectNamespace["newUniqueId"]>,
    Parameters<DurableObjectNamespace["newUniqueId"]>
  >(),
  idFromName: jest.fn<
    ReturnType<DurableObjectNamespace["idFromName"]>,
    Parameters<DurableObjectNamespace["idFromName"]>
  >(),
  idFromString: jest.fn<
    ReturnType<DurableObjectNamespace["idFromString"]>,
    Parameters<DurableObjectNamespace["idFromString"]>
  >(),
  get: jest.fn<
    ReturnType<DurableObjectNamespace["get"]>,
    Parameters<DurableObjectNamespace["get"]>
  >(),
});

export const mockDOStub = () => ({
  fetch: jest.fn<
    ReturnType<DurableObjectStub["fetch"]>,
    Parameters<DurableObjectStub["fetch"]>
  >(),
  id: mockDOId(),
});

export const mockDOId = () => ({
  toString: jest.fn<
    ReturnType<DurableObjectId["toString"]>,
    Parameters<DurableObjectId["toString"]>
  >(),
  equals: jest.fn<
    ReturnType<DurableObjectId["equals"]>,
    Parameters<DurableObjectId["equals"]>
  >(),
});

export const mockDOState = () => ({
  waitUntil: jest.fn<
    ReturnType<DurableObjectState["waitUntil"]>,
    Parameters<DurableObjectState["waitUntil"]>
  >(),
  id: mockDOId(),
  storage: mockDOStorage(),
  // Can't handle generics here?
  blockConcurrencyWhile: jest.fn(),
});

export const mockDOStorage = () => ({
  // Can't handle generics here?
  get: jest.fn(),
  // Can't handle generics here?
  list: jest.fn(),
  // Can't handle generics here?
  put: jest.fn(),
  // Can't handle overloading here?
  delete: jest.fn(),
  deleteAll: jest.fn<
    ReturnType<DurableObjectStorage["deleteAll"]>,
    Parameters<DurableObjectStorage["deleteAll"]>
  >(),
  // Can't handle generics here?
  transaction: jest.fn(),
  getAlarm: jest.fn<
    ReturnType<DurableObjectStorage["getAlarm"]>,
    Parameters<DurableObjectStorage["getAlarm"]>
  >(),
  setAlarm: jest.fn<
    ReturnType<DurableObjectStorage["setAlarm"]>,
    Parameters<DurableObjectStorage["setAlarm"]>
  >(),
  deleteAlarm: jest.fn<
    ReturnType<DurableObjectStorage["deleteAlarm"]>,
    Parameters<DurableObjectStorage["deleteAlarm"]>
  >(),
});
