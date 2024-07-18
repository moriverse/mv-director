from attrs import define
from cog import schema
from cog.server.http import Health
from typing import Any, Dict, Optional


@define
class Webhook:
    payload: schema.PredictionResponse


@define
class HealthcheckStatus:
    health: Health
    metadata: Optional[Dict[str, Any]] = None