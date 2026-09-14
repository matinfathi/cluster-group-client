import asyncio
from cluster_group_client.main import create_group, delete_group, get_group

__all__ = ["create_group", "delete_group", "get_group", "main"]


def main() -> None:
    result = asyncio.run(create_group("sample-id"))
    print(result)
