from typing import Optional

from sqlalchemy import *
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase


class Base(DeclarativeBase):
    pass


class Test1(Base):
    __tablename__ = 'test_1'
    __table_args__ = (
        Index('u_id', 'a', unique=True),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    a: Mapped[Optional[int]] = mapped_column(Integer)
    b: Mapped[Optional[int]] = mapped_column(Integer)
    c: Mapped[Optional[int]] = mapped_column(Integer)
    d: Mapped[Optional[int]] = mapped_column(Integer)
