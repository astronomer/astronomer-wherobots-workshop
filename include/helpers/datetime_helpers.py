from datetime import datetime, timedelta
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


def generate_daily_dates(start_date: str, end_date: str) -> List[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    dates = []
    current = start

    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    logger.debug(f"Created {len(dates)} daily partitions")
    logger.debug(f"Dates: {dates[:10]}...")

    return dates
