from sqlalchemy import Column, String, Integer, DateTime
from database import Base
from datetime import datetime, timezone


class TradingResult(Base):
    """
    Модель для представления результатов торгов на СПбМТСБ.

    Attributes:
        id (int): Первичный ключ.
        exchange_product_id (str): Идентификатор биржевого продукта.
        exchange_product_name (str): Название биржевого продукта.
        oil_id (str): Идентификатор нефти.
        delivery_basis_id (str): Идентификатор базиса поставки.
        delivery_basis_name (str): Название базиса поставки.
        delivery_type_id (str): Идентификатор типа поставки.
        volume (int): Объем.
        total (int): Всего.
        count (int): Количество.
        date (datetime): Дата.
        created_on (datetime): Дата и время создания записи.
        updated_on (datetime): Дата и время последнего обновления записи.
    """
    __tablename__ = "spimex_trading_result_async"
    id = Column(Integer, primary_key=True, index=True)
    exchange_product_id = Column(String)
    exchange_product_name = Column(String)
    oil_id = Column(String)
    delivery_basis_id = Column(String)
    delivery_basis_name = Column(String)
    delivery_type_id = Column(String)
    volume = Column(Integer)
    total = Column(Integer)
    count = Column(Integer)
    date = Column(DateTime)
    created_on = Column(DateTime, default=datetime.now(timezone.utc))
    updated_on = Column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
