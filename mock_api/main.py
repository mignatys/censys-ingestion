import random
import time
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

app = FastAPI(title="Mock Upstream API", version="0.1.0")

class Severity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class Source(str, Enum):
    # Standard Group
    FIREWALL = "firewall"
    NGFW = "ngfw"
    IPS = "ips"
    VPN_GATEWAY = "vpn_gateway"
    ROUTER_ACL = "router_acl"
    
    # Legacy Group
    WAF = "waf"
    SWG = "swg"
    NAC = "nac"
    API_GATEWAY = "api_gateway"
    LOAD_BALANCER = "load_balancer"

LEGACY_SOURCES = {Source.WAF, Source.SWG, Source.NAC, Source.API_GATEWAY, Source.LOAD_BALANCER}

class Config(BaseModel):
    failure_rate: float = Field(0.2, ge=0.0, le=1.0)

# Global Configuration State
current_config = Config()

@app.post("/config")
async def update_config(config: Config):
    """Update chaos engineering parameters."""
    current_config = config
    return {"message": "Config updated", "config": current_config}

@app.get("/config")
async def get_config():
    """Retrieve current chaos settings."""
    return current_config

@app.get("/alerts")
async def get_alerts(since: Optional[str] = Query(None, description="ISO8601 timestamp to filter alerts")):
    """
    Fetch security alerts.
    Simulates a flaky API with random 500 errors, delays, and MIXED FORMATS.
    """
    
    # Add a little delay to see state propgation in ui
    time.sleep(3)

    # Chaos Engineering: Failure Rate
    if random.random() < current_config.failure_rate:
        raise HTTPException(status_code=500, detail="Internal Server Error (Simulated)")

    # Generate 1-5 random alerts
    num_alerts = random.randint(1, 5)
    alerts = []
    
    for i in range(num_alerts):
        alert_id = f"alert-{int(time.time())}-{i}" # Simple unique ID
        source = random.choice(list(Source))
        created_at = datetime.now(timezone.utc)
        
        # Generate IPs
        src_ip = f"192.168.{random.randint(0, 255)}.{random.randint(1, 255)}"
        dst_ip = f"{random.randint(50, 150)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"
        
        if source in LEGACY_SOURCES:
            # Legacy Format (Group B)
            # Map severity to criticality (1-4)
            criticality = random.randint(1, 4) 
            
            alerts.append({
                "alertId": alert_id,
                "source": source.value,
                "criticality": criticality,
                "summary": f"Legacy event from {source} with criticality {criticality}.",
                "sourceAddress": src_ip,
                "destinationAddress": dst_ip,
                "timestamp": created_at.isoformat()
            })
        else:
            # Standard Format (Group A)
            severity = random.choice(list(Severity))
            description = f"Standard security event from {source} with {severity} severity."
            
            alerts.append({
                "id": alert_id,
                "source": source.value,
                "severity": severity.value,
                "description": description,
                "src_ip": src_ip,
                "dst_ip": dst_ip,
                "created_at": created_at.isoformat()
            })

    return alerts

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
