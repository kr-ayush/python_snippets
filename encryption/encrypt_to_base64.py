import base64
import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy.types import TypeDecorator
from sqlalchemy.orm import sessionmaker, declarative_base
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder

url = URL.create(
    drivername="postgresql",
    username="postgres",
    password="postgres",
    host="localhost",
    database="postgres",
    port=5432,
)

engine = sa.create_engine(url)
Session = sessionmaker(bind=engine)
session = Session()


Base = declarative_base()


class EncType(TypeDecorator):
    impl = sa.String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if isinstance(value, str):
            value = str.encode(value, encoding="utf-8")
        return base64.urlsafe_b64encode(value).decode(encoding="utf-8")

    def process_result_value(self, value, dialect):
        if isinstance(value, str):
            value = str.encode(value, encoding="utf-8")
        return base64.urlsafe_b64decode(value)


class TestTable(Base):
    __tablename__ = "test_encryption"

    id = sa.Column(
        sa.Integer,
        primary_key=True,
        index=True,
    )
    name = sa.Column(sa.String)
    enc_name = sa.Column(EncType)


Base.metadata.create_all(engine)

app = FastAPI()


@app.get("/")
async def get_encrpted_entries():
    result = session.query(TestTable).all()
    if result:
        result = jsonable_encoder(result)
    return result


@app.post("/")
async def create_encrypted_entries():
    obj = TestTable(name="Ayush", enc_name="Ayush")
    print(str.encode("Ayush"))
    session.add(obj)
    session.commit()
    return "Added"
