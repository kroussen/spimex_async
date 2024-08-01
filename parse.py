import pandas as pd
from datetime import datetime
from io import BytesIO
from typing import List

from database import save_to_database
from models import TradingResult
from template_data import column_indexes, column_names_mapping


def is_unit_of_measurement_metric_ton(file: pd.DataFrame, row: int, col: int) -> bool:
    """
    Проверяет, является ли единица измерения метрической тонной.

    Args:
        file (pd.DataFrame): DataFrame, содержащий данные.
        row (int): Индекс строки для проверки.
        col (int): Индекс столбца для проверки.

    Returns:
        bool: True, если единица измерения - метрическая тонна, иначе False.
    """
    return file.iloc[row].values[col] == 'Единица измерения: Метрическая тонна'


def removing_headers_and_totals(file: pd.DataFrame, start_row: int, end_row: int) -> pd.DataFrame:
    """
    Удаляет заголовки и итоговые строки из DataFrame.

    Args:
        file (pd.DataFrame): DataFrame, содержащий данные.
        start_row (int): Начальная строка для удаления заголовков.
        end_row (int): Конечная строка для удаления итоговых строк.

    Returns:
        pd.DataFrame: DataFrame без заголовков и итоговых строк.
    """
    data_without_headers_and_totals = file.iloc[start_row:end_row]
    return data_without_headers_and_totals


def get_positive_data_by_column(file: pd.DataFrame, column: int) -> pd.DataFrame:
    """
    Получает данные из столбца, содержащие положительные значения.

    Args:
        file (pd.DataFrame): DataFrame, содержащий данные.
        column (int): Индекс столбца для проверки положительных значений.

    Returns:
        pd.DataFrame: DataFrame с положительными значениями в указанном столбце.
    """
    contracts_with_multiple_entries = file[file.iloc[:, column].values != '-']
    return contracts_with_multiple_entries


async def fetch_excel_and_process(session, url: str, file_date: str):
    """
    Загружает Excel файл по указанному URL, обрабатывает его и сохраняет данные в базу данных.

    Args:
        session: HTTP сессия для выполнения запроса.
        url (str): URL для загрузки Excel файла.
        file_date (str): Дата файла.

    Returns:
        None
    """
    async with session.get(url) as response:
        if response.status == 200:
            content: bytes = await response.read()
            df: pd.DataFrame = pd.read_excel(BytesIO(content))
            if is_unit_of_measurement_metric_ton(df, row=4, col=1):
                data_without_headers_and_totals: pd.DataFrame = removing_headers_and_totals(df, start_row=7, end_row=-2)
                contracts_with_positive_count: pd.DataFrame = get_positive_data_by_column(
                    data_without_headers_and_totals, column=14)
                data_to_insert: List[TradingResult] = []

                for col in contracts_with_positive_count.iloc[:, list(column_indexes.values())].values:
                    exchange_product_id: str = col[column_names_mapping['exchange_product_id']]
                    exchange_product_name: str = col[column_names_mapping['exchange_product_name']]
                    delivery_basis_name: str = col[column_names_mapping['delivery_basis_name']]
                    volume: int = int(col[column_names_mapping['volume']])
                    total: int = int(col[column_names_mapping['total']])
                    count: int = int(col[column_names_mapping['count']])

                    data: TradingResult = TradingResult(
                        exchange_product_id=exchange_product_id,
                        exchange_product_name=exchange_product_name,
                        oil_id=exchange_product_id[:4],
                        delivery_basis_id=exchange_product_id[4:7],
                        delivery_basis_name=delivery_basis_name,
                        delivery_type_id=exchange_product_id[-1],
                        volume=volume,
                        total=total,
                        count=count,
                        created_on=datetime.now(),
                        updated_on=datetime.now()
                    )
                    data_to_insert.append(data)
                await save_to_database(data_to_insert)
