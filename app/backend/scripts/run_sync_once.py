from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from gisdatamonitor_backend.config import get_settings  # noqa: E402
from gisdatamonitor_backend.db import get_engine  # noqa: E402
from gisdatamonitor_backend.services.sync import EventSyncService  # noqa: E402


def main() -> None:
    settings = get_settings()
    service = EventSyncService(engine=get_engine(), settings=settings)
    result = service.run_cycle()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

