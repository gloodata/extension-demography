import aiosql
import duckdb
import logging
from enum import Enum
from glootil import DynEnum, identity

log = logging.getLogger("state")


class SQLStore:
    def __init__(
        self,
        driver_adapter,
        queries_path="queries.sql",
        driver_args=None,
        driver_kwargs=None,
    ):
        self.conn = None
        self.queries = None
        self.queries_path = queries_path
        self.driver_adapter = driver_adapter
        self.driver_args = driver_args
        self.driver_kwargs = driver_kwargs

    async def connect(self):
        log.warning("connect not implemented")

    async def disconnect(self):
        log.warning("disconnect not implemented")

    async def pre_setup(self):
        pass

    async def post_setup(self):
        pass

    async def pre_dispose(self):
        pass

    async def post_dispose(self):
        pass

    async def setup(self):
        await self.pre_setup()
        args = self.driver_args or []
        kwargs = self.driver_kwargs or {}
        self.queries = aiosql.from_path(
            self.queries_path, self.driver_adapter, args=args, kwargs=kwargs
        )
        await self.connect()
        await self.post_setup()

    async def dispose(self):
        await self.pre_dispose()
        await self.disconnect()
        await self.post_dispose()

    async def query_to_tuple(self, query_name_, query_args_=None, **kwargs):
        return await self.query(
            query_name_, query_args_, keyseq_to_tuple(list(kwargs.items()))
        )

    async def query_to_tuple_from_col_names(
        self, query_name, query_args=None, col_names=None
    ):
        cols = [(name, None) for name in (col_names or [])]
        return await self.query(query_name, query_args, keyseq_to_tuple(cols))

    async def run_query_fn(self, _name, query_fn, query_args):
        return await query_fn(self.conn, **query_args)

    async def query(self, name, args=None, map=identity, verbose=False):
        args = args if args is not None else {}
        query_args = {key: to_query_arg(value) for key, value in args.items()}

        if verbose:
            log.info("running %s: %s", name, query_args)

        query_fn = getattr(self.queries, name)
        if query_fn:
            rows = map(await self.run_query_fn(name, query_fn, query_args))
        else:
            log.warning("Query '%s' not found", name)
            rows = []

        if verbose:
            log.debug("result: %s", rows)

        return rows

    async def query_one(
        self, name, args=None, map=identity, verbose=False, default=None
    ):
        rows = await self.query(name, args, map, verbose)
        return rows[0] if rows else default


class DuckStore(SQLStore):
    def __init__(self, queries_path="queries.sql"):
        super().__init__("duckdb", queries_path, driver_kwargs={"cursor_as_dict": True})

    async def run_query_fn(self, name, query_fn, query_args):
        # query result is a generator
        return [row for row in query_fn(self.conn, **query_args)]

    async def connect(self):
        self.conn = duckdb.connect()

    async def disconnect(self):
        self.conn.close()


def dict_factory(cursor, row):
    r = {}
    i = 0
    for column in cursor.description:
        key = column[0]
        r[key] = row[i]
        i += 1

    return r


def keys_to_tuple(**keys):
    keyseq = list(keys.items())
    return keyseq_to_tuple(keyseq)


def columns_info_to_tuple(cols):
    keyseq = [(col.get("id"), col.get("default")) for col in cols]
    return keyseq_to_tuple(keyseq)


def keyseq_to_tuple(keyseq):
    def key_selector(row):
        return [row.get(key, defval) for key, defval in keyseq]

    def f(r):
        if isinstance(r, list):
            return [key_selector(row) for row in r]
        else:
            return key_selector(r)

    return f


def to_query_arg(value):
    if value is None or isinstance(value, (str, int, bool)):
        return value
    elif isinstance(value, (Enum, DynEnum)):
        return value.name
    else:
        return str(value)
