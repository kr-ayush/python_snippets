from time import perf_counter
import faker as fk
import psycopg2 as pg
from psycopg2.extras import execute_batch


def create_psycopg2_connection():
    conn = pg.connect(
        database="postgres",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432",
    )
    return conn


def create_fake_entries(values: int = 10000) -> list:
    result = []
    fake = fk.Faker()
    for _ in range(0, values):
        result.append(fake.profile())
    return result


def postgres_to_sql_execute(resp: list):
    conn = create_psycopg2_connection()
    function_start = perf_counter()
    cur = conn.cursor()
    args_str = b",".join(
        cur.mogrify(
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            tuple(x.values()),
        )
        for x in resp
    )
    sql_start = perf_counter()
    cur.execute(
        b"""INSERT INTO public.profiles(
            job,
            company,
            ssn,
            residence,
            current_location,
            blood_group,
            website,
            username,
            name,
            sex,
            address,
            mail,
            birthdate
            )
        VALUES """
        + args_str
    )
    conn.commit()
    sql_end = perf_counter()
    conn.close()
    function_end = perf_counter()
    print("Function Time: ", function_end - function_start)
    print("SQL Time: ", sql_end - sql_start)


def postgres_to_sql_batch_execute(resp: list):
    conn = create_psycopg2_connection()
    function_start = perf_counter()
    cur = conn.cursor()
    values = [tuple(row.values()) for row in resp]
    script = b"""INSERT INTO public.profiles(
        job,
        company,
        ssn,
        residence,
        current_location,
        blood_group,
        website,
        username,
        name,
        sex,
        address,
        mail,
        birthdate
    )
    VALUES  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    sql_start = perf_counter()
    execute_batch(cur, script, values)
    conn.commit()
    sql_end = perf_counter()
    conn.close()
    function_end = perf_counter()
    print("Function Time: ", function_end - function_start)
    print("SQL Time: ", sql_end - sql_start)


if __name__ == "__main__":
    resp = create_fake_entries(values=10000)
    print("SQL Execute + morgify")
    postgres_to_sql_execute(resp=resp)
    print("SQL BATCH EXECUTE")
    resp = create_fake_entries(values=10000)
    postgres_to_sql_batch_execute(resp=resp)
