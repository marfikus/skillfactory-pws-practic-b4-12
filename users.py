
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

DB_PATH = "sqlite:///sochi_athletes.sqlite3"

Base = declarative_base()

class User(Base):
    __tablename__ = "user"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    first_name = sa.Column(sa.Text)
    last_name = sa.Column(sa.Text)
    gender = sa.Column(sa.Text)
    email = sa.Column(sa.Text)
    birthdate = sa.Column(sa.Text)
    height = sa.Column(sa.Float)

def connect_db():
    # создаём подключение к бд
    engine = sa.create_engine(DB_PATH)
    # создаём описанные таблицы
    Base.metadata.create_all(engine)
    # создаём фабрику сессий
    Sessions = sessionmaker(engine)
    # создаём сессию
    session = Sessions()
    return session

