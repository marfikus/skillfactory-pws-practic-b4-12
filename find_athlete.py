
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

class Athelete(Base):
    __tablename__ = "athelete"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    age = sa.Column(sa.Integer)
    birthdate = sa.Column(sa.Text)
    gender = sa.Column(sa.Text)
    height = sa.Column(sa.Float)
    name = sa.Column(sa.Text)
    weight = sa.Column(sa.Integer)
    gold_medals = sa.Column(sa.Integer)
    silver_medals = sa.Column(sa.Integer)
    bronze_medals = sa.Column(sa.Integer)
    total_medals = sa.Column(sa.Integer)
    sport = sa.Column(sa.Text)
    country = sa.Column(sa.Text)

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

def main():
    session = connect_db()

    user_id = input("Enter user id: ")

    user = session.query(User).filter(User.id == user_id).first()
    if user is None:
        print("No user with id:", user_id)
        return

    # print(user.first_name)

    # ищем точное совпадение роста:
    athlete = session.query(Athelete).filter(Athelete.height == user.height).first()
    if not athlete is None:
        print(athlete.name)
        return

    athletes = session.query(Athelete).all()
    # print(len(athletes))
    # print(athletes[0].height)
    print("user.height:", user.height)
    min_dif_heights = abs(user.height - athletes[0].height)
    # print(min_dif_heights)
    for athlete in athletes:
        if athlete.height is None:
            continue
        dif_heights = abs(user.height - athlete.height)
        if dif_heights < min_dif_heights:
            min_dif_heights = dif_heights

    print("min_dif_heights:", min_dif_heights)


if __name__ == "__main__":
    main()
