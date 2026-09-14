import asyncio
import logging
import os
from dotenv import load_dotenv
import httpx

load_dotenv()

logger = logging.getLogger(__name__)


def get_node_urls() -> list[str]:
    raw_urls = os.getenv("NODE_URLS", "")
    return [url.strip() for url in raw_urls.split(",") if url.strip()]


async def _check_group_exists(client: httpx.AsyncClient, node_url: str, group_id: str) -> bool:
    endpoint = f"{node_url.rstrip('/')}/v1/group/{group_id}/"
    try:
        response = await client.get(endpoint, timeout=5.0)
        return response.status_code == 200
    except httpx.RequestError:
        return False


async def create_on_node(client: httpx.AsyncClient, node_url: str, payload: dict) -> tuple[str, bool]:
    endpoint = f"{node_url.rstrip('/')}/v1/group/"
    delays = [0.5, 1.0]

    for attempt in range(3):
        try:
            response = await client.post(endpoint, json=payload, timeout=5.0)
            if response.is_success:
                return node_url, True
            if response.status_code == 400:
                exists = await _check_group_exists(client, node_url, payload["groupId"])
                if exists:
                    logger.info("Group already exists on %s, treating as success", node_url)
                    return node_url, True
                logger.warning("Create got 400 on %s but group does not exist", node_url)
                return node_url, False
            logger.warning("Create attempt %d/3 on %s: status=%d", attempt + 1, node_url, response.status_code)
        except httpx.RequestError as exc:
            logger.warning("Create attempt %d/3 on %s: %s", attempt + 1, node_url, exc)

        if attempt < 2:
            await asyncio.sleep(delays[attempt])

    return node_url, False


async def rollback_on_node(client: httpx.AsyncClient, node_url: str, group_id: str) -> tuple[str, bool]:
    endpoint = f"{node_url.rstrip('/')}/v1/group/"
    delays = [0.5, 1.0, 2.0, 4.0]

    for attempt in range(5):
        try:
            response = await client.delete(endpoint, json={"groupId": group_id}, timeout=5.0)
            if response.is_success or response.status_code == 404:
                return node_url, True
            logger.warning("Rollback attempt %d/5 on %s: status=%d", attempt + 1, node_url, response.status_code)
        except httpx.RequestError as exc:
            logger.warning("Rollback attempt %d/5 on %s: %s", attempt + 1, node_url, exc)

        if attempt < 4:
            await asyncio.sleep(delays[attempt])

    logger.error("Rollback failed on %s for group %s after 5 attempts", node_url, group_id)
    return node_url, False


async def delete_on_node(client: httpx.AsyncClient, node_url: str, group_id: str) -> tuple[str, bool]:
    endpoint = f"{node_url.rstrip('/')}/v1/group/"
    delays = [0.5, 1.0, 2.0, 4.0]

    for attempt in range(5):
        try:
            response = await client.delete(endpoint, json={"groupId": group_id}, timeout=5.0)
            if response.is_success:
                return node_url, True
            logger.warning("Delete attempt %d/5 on %s: status=%d", attempt + 1, node_url, response.status_code)
        except httpx.RequestError as exc:
            logger.warning("Delete attempt %d/5 on %s: %s", attempt + 1, node_url, exc)

        if attempt < 4:
            await asyncio.sleep(delays[attempt])

    logger.error("Delete failed on %s for group %s after 5 attempts", node_url, group_id)
    return node_url, False


async def get_on_node(client: httpx.AsyncClient, node_url: str, group_id: str) -> tuple[str, dict | None]:
    endpoint = f"{node_url.rstrip('/')}/v1/group/{group_id}/"
    try:
        response = await client.get(endpoint, timeout=5.0)
        if response.is_success:
            return node_url, response.json()
    except httpx.RequestError:
        pass
    return node_url, None


async def create_group(group_id: str, node_urls: list[str] | None = None) -> dict:
    nodes = node_urls if node_urls is not None else get_node_urls()
    if not nodes:
        raise ValueError("No node addresses configured in NODE_URLS.")

    logger.info("Creating group %s on %d nodes", group_id, len(nodes))
    payload = {"groupId": group_id}

    async with httpx.AsyncClient() as client:
        create_tasks = [create_on_node(client, node, payload) for node in nodes]
        create_results = dict(await asyncio.gather(*create_tasks))

        if all(create_results.values()):
            logger.info("Group %s created on all nodes", group_id)
            return {
                "success": True,
                "created_nodes": create_results,
                "deleted_nodes": {},
            }

        # Verify failed nodes — they might have actually created the group
        failed_nodes = [node for node, ok in create_results.items() if not ok]
        verify_tasks = [_check_group_exists(client, node, group_id) for node in failed_nodes]
        verify_results = dict(zip(failed_nodes, await asyncio.gather(*verify_tasks)))

        for node, exists in verify_results.items():
            if exists:
                logger.info("Group %s verified to exist on %s despite create failure", group_id, node)
                create_results[node] = True

        if all(create_results.values()):
            logger.info("Group %s confirmed on all nodes after verification", group_id)
            return {
                "success": True,
                "created_nodes": create_results,
                "deleted_nodes": {},
            }

        # Rollback all nodes where group actually exists
        nodes_to_rollback = [node for node, ok in create_results.items() if ok]
        logger.error(
            "Create failed for group %s — rolling back %d nodes",
            group_id, len(nodes_to_rollback),
        )
        rollback_tasks = [rollback_on_node(client, node, group_id) for node in nodes_to_rollback]
        rollback_results = dict(await asyncio.gather(*rollback_tasks))

        logger.info("Rollback complete for group %s: %s", group_id, rollback_results)
        return {
            "success": False,
            "created_nodes": create_results,
            "deleted_nodes": rollback_results,
        }


async def delete_group(group_id: str, node_urls: list[str] | None = None) -> dict:
    nodes = node_urls if node_urls is not None else get_node_urls()
    if not nodes:
        raise ValueError("No node addresses configured in NODE_URLS.")

    logger.info("Deleting group %s from %d nodes", group_id, len(nodes))

    async with httpx.AsyncClient() as client:
        delete_tasks = [delete_on_node(client, node, group_id) for node in nodes]
        delete_results = dict(await asyncio.gather(*delete_tasks))

        success = all(delete_results.values())
        if success:
            logger.info("Group %s deleted from all nodes", group_id)
        else:
            logger.error("Delete failed for group %s on some nodes: %s", group_id, delete_results)

        return {
            "success": success,
            "deleted_nodes": delete_results,
        }


async def get_group(group_id: str, node_urls: list[str] | None = None) -> dict:
    nodes = node_urls if node_urls is not None else get_node_urls()
    if not nodes:
        raise ValueError("No node addresses configured in NODE_URLS.")

    async with httpx.AsyncClient() as client:
        get_tasks = [get_on_node(client, node, group_id) for node in nodes]
        results = dict(await asyncio.gather(*get_tasks))
        return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = asyncio.run(create_group("sample-id"))
    print(result)
