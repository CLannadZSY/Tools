from sqlalchemy import *
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Test1(Base):
    __tablename__ = 'test_1'

    id = Column(Integer, primary_key=True)
    a = Column(String(255, collation='utf8mb4_unicode_ci'))
    b = Column(String(255, collation='utf8mb4_unicode_ci'))
    c = Column(String(255, collation='utf8mb4_unicode_ci'))
    d = Column(String(255, collation='utf8mb4_unicode_ci'))
