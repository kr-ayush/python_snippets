from time import perf_counter
import faker as fk
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL


def create_sqlalchemy_connection():
    url = URL.create(
        drivername="postgresql",
        username="postgres",
        password="postgres",
        host="localhost",
        database="postgres",
        port=5432,
    )
    engine = sa.create_engine(url)
    return engine


def create_fake_entries(values: int = 10000) -> list:
    result = []
    fake = fk.Faker()
    for _ in range(0, values):
        result.append(fake.profile())
    return result


def pandas_to_sql(resp: list):
    try:
        engine = create_sqlalchemy_connection()
        function_start = perf_counter()
        resp_df = pd.DataFrame(resp)
        with engine.connect() as conn:
            sql_start = perf_counter()
            resp_df.to_sql(
                name="profiles",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=1000,
            )
            sql_end = perf_counter()
        function_end = perf_counter()
        print("Function Time: ", function_end - function_start)
        print("SQL Time: ", sql_end - sql_start)
    except Exception as err:
        print(err)


if __name__ == "__main__":
    resp = create_fake_entries(values=10000)
    print("Creating Entires Using to_sql() function")
    pandas_to_sql(resp=resp)
