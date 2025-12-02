import requests
import os
import docker

def check_service_health():
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')

    services = ["minio", "rabbitmq", "extractor", "transformer", "cleaner"]
    health = {}

    for service in services:
        try:
            container = client.containers.get(service)
            state = container.attrs["State"]

            if "Health" in state:
                status = state["Health"]["Status"]
                health[service] = f"🟢 {status}" if status == "healthy" else f"🟠 {status}"
            elif state["Running"]:
                health[service] = f"🟢 {status}" 
            else:
                health[service] = "🔴 Stopped"
        except Exception as e:
            health[service] = f"❌ Error Service not Responding)"

    return health