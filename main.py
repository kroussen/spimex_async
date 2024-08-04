import asyncio
import aiohttp

from time import time
from datetime import datetime, timedelta

from database import create_tables
from parse import fetch_excel_and_process


async def generate_urls_and_fetch(start_date: datetime, end_date: datetime) -> None:
    """
    Генерирует URL для заданного диапазона дат и выполняет асинхронную загрузку и обработку файлов.

    Args:
        start_date (datetime): Начальная дата.
        end_date (datetime): Конечная дата.

    Returns:
        None
    """
    tasks = []
    async with aiohttp.ClientSession() as session:
        current_date = start_date
        while current_date < end_date:
            date_str = current_date.strftime('%Y%m%d')
            url = f'https://spimex.com/upload/reports/oil_xls/oil_xls_{date_str}162000.xls'
            tasks.append(fetch_excel_and_process(session, url, current_date))
            current_date += timedelta(days=1)
        await asyncio.gather(*tasks)


async def main() -> None:
    """
    Основная асинхронная функция, которая создает таблицы и инициирует процесс загрузки и обработки файлов.

    Returns:
        None
    """
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 6, 1)
    await create_tables()
    await generate_urls_and_fetch(start_date, end_date)


if __name__ == "__main__":
    t0 = time()
    asyncio.run(main())
    print(f"Время выполнения: {time() - t0} секунд")
