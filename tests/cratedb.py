import typing as t

import sqlalchemy as sa
from sqlalchemy_cratedb.support import quote_relation_name
from testcontainers.community.cratedb import CrateDBContainer


class CrateDBTestAdapter:
    """
    Start a CrateDB container and run SQL against it.
    """

    def __init__(self, crate_version: str = "nightly") -> None:
        # Only `crate/crate` has nightly builds; its release tags are amd64-only, the official `crate` image is not.
        self.image = "crate/crate:nightly" if crate_version == "nightly" else f"crate:{crate_version}"

    def start(self) -> None:
        self.cratedb = CrateDBContainer(image=self.image)
        self.cratedb.start()
        self.engine = sa.create_engine(self.cratedb.get_connection_url())

    def stop(self) -> None:
        self.engine.dispose()
        self.cratedb.stop()

    def reset(self, tables: t.List[str]) -> None:
        for table in tables:
            self.run_sql(f"DROP TABLE IF EXISTS {quote_relation_name(table)};")

    def run_sql(
        self,
        sql: str,
        parameters: t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]], None] = None,
        records: bool = False,
    ):
        with self.engine.begin() as connection:
            result = connection.execute(sa.text(sql), parameters)
            if records:
                return [dict(row) for row in result.mappings().fetchall()]
            return result.fetchall()

    def count_records(self, name: str) -> int:
        return self.run_sql(f"SELECT COUNT(*) FROM {quote_relation_name(name)};")[0][0]

    def table_exists(self, name: str) -> bool:
        try:
            self.run_sql(f"SELECT 1 FROM {quote_relation_name(name)} LIMIT 1;")
        except sa.exc.ProgrammingError:
            return False
        return True

    def refresh_table(self, name: str) -> bool:
        self.run_sql(f"REFRESH TABLE {quote_relation_name(name)};")
        return True
