from time import perf_counter
import faker as fk
import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy.orm import declarative_base, sessionmaker


url = URL.create(
    drivername="postgresql",
    username="postgres",
    password="postgres",
    host="localhost",
    database="postgres",
    port=5432,
)
engine = sa.create_engine(url)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


def yield_db():
    try:
        session = SessionLocal()
        yield session
    finally:
        session.close()


class Profile(Base):
    __tablename__ = "profiles2"

    id = sa.Column(sa.Integer, primary_key=True, index=True)
    job = sa.Column(sa.String, nullable=True)
    company = sa.Column(sa.String, nullable=True)
    ssn = sa.Column(sa.String, nullable=True)
    residence = sa.Column(sa.String, nullable=True)
    current_location = sa.Column(sa.String, nullable=True)
    blood_group = sa.Column(sa.String, nullable=True)
    website = sa.Column(sa.String, nullable=True)
    username = sa.Column(sa.String, nullable=True)
    name = sa.Column(sa.String, nullable=True)
    sex = sa.Column(sa.String, nullable=True)
    address = sa.Column(sa.String, nullable=True)
    mail = sa.Column(sa.String, nullable=True)
    birthdate = sa.Column(sa.DateTime, nullable=True)


Base.metadata.create_all(bind=engine)


def create_fake_entries(values: int = 10000) -> list:
    result = []
    fake = fk.Faker()
    for _ in range(0, values):
        result.append(fake.profile())
    return result


def create_sqlalchemy_entries(result: list):
    try:
        function_start = perf_counter()
        obj_list = [
            Profile(
                job=x["job"],
                company=x["company"],
                ssn=x["ssn"],
                residence=x["residence"],
                current_location=x["current_location"],
                blood_group=x["blood_group"],
                website=x["website"],
                username=x["username"],
                name=x["name"],
                sex=x["sex"],
                address=x["address"],
                mail=x["mail"],
                birthdate=x["birthdate"],
            )
            for x in result
        ]
        session = next(yield_db())
        sql_start = perf_counter()
        with session.begin():
            session.add_all(obj_list)
            session.commit()
        sql_end = perf_counter()
        session.close()
        function_end = perf_counter()
        print("Function Time: ", function_end - function_start)
        print("SQL Time: ", sql_end - sql_start)

    except Exception as err:
        print(err)


if __name__ == "__main__":
    resp = create_fake_entries(values=10000)
    print("Using ORM add_all() function")
    create_sqlalchemy_entries(result=resp)
