from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, call, patch

import httpx

from cluster_group_client.main import (
    create_group,
    create_on_node,
    delete_group,
    delete_on_node,
    get_group,
    get_on_node,
    rollback_on_node,
)

NODES = ["http://node1:8000", "http://node2:8000"]


class TestCreateOnNode(IsolatedAsyncioTestCase):
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_success_first_try(self, _):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_http.post.return_value = httpx.Response(201, request=httpx.Request("POST", "http://test"))

        node, success = await create_on_node(mock_http, "http://node1:8000", {"groupId": "g-1"})

        self.assertTrue(success)
        self.assertEqual(mock_http.post.call_count, 1)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_retry_then_success(self, _):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        req = httpx.Request("POST", "http://test")
        mock_http.post.side_effect = [
            httpx.ConnectError("Failed", request=req),
            httpx.Response(500, request=req),
            httpx.Response(201, request=req),
        ]

        node, success = await create_on_node(mock_http, "http://node1:8000", {"groupId": "g-1"})

        self.assertTrue(success)
        self.assertEqual(mock_http.post.call_count, 3)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_fails_after_3_retries(self, _):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        req = httpx.Request("POST", "http://test")
        mock_http.post.side_effect = [httpx.ConnectError("Failed", request=req)] * 3

        node, success = await create_on_node(mock_http, "http://node1:8000", {"groupId": "g-1"})

        self.assertFalse(success)
        self.assertEqual(mock_http.post.call_count, 3)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_exponential_backoff_delays(self, mock_sleep):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        req = httpx.Request("POST", "http://test")
        mock_http.post.side_effect = [httpx.ConnectError("Failed", request=req)] * 3

        await create_on_node(mock_http, "http://node1:8000", {"groupId": "g-1"})

        mock_sleep.assert_has_calls([call(0.5), call(1.0)])

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_400_with_existing_group_returns_success(self, _):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        req = httpx.Request("POST", "http://test")
        mock_http.post.return_value = httpx.Response(400, request=req)
        mock_http.get.return_value = httpx.Response(
            200, json={"groupId": "g-1"}, request=httpx.Request("GET", "http://test"),
        )

        node, success = await create_on_node(mock_http, "http://node1:8000", {"groupId": "g-1"})

        self.assertTrue(success)
        self.assertEqual(mock_http.post.call_count, 1)
        mock_http.get.assert_called_once()

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_400_with_no_existing_group_returns_failure(self, _):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        req = httpx.Request("POST", "http://test")
        mock_http.post.return_value = httpx.Response(400, request=req)
        mock_http.get.return_value = httpx.Response(404, request=httpx.Request("GET", "http://test"))

        node, success = await create_on_node(mock_http, "http://node1:8000", {"groupId": "g-1"})

        self.assertFalse(success)
        self.assertEqual(mock_http.post.call_count, 1)
        mock_http.get.assert_called_once()

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_400_with_get_connection_error_returns_failure(self, _):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        req = httpx.Request("POST", "http://test")
        mock_http.post.return_value = httpx.Response(400, request=req)
        mock_http.get.side_effect = httpx.ConnectError("Failed", request=httpx.Request("GET", "http://test"))

        node, success = await create_on_node(mock_http, "http://node1:8000", {"groupId": "g-1"})

        self.assertFalse(success)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_timeout_error_is_retried(self, _):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        req = httpx.Request("POST", "http://test")
        mock_http.post.side_effect = [
            httpx.ReadTimeout("Timeout", request=req),
            httpx.Response(201, request=req),
        ]

        node, success = await create_on_node(mock_http, "http://node1:8000", {"groupId": "g-1"})

        self.assertTrue(success)
        self.assertEqual(mock_http.post.call_count, 2)


class TestRollbackOnNode(IsolatedAsyncioTestCase):
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_success(self, _):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_http.delete.return_value = httpx.Response(200, request=httpx.Request("DELETE", "http://test"))

        node, success = await rollback_on_node(mock_http, "http://node1:8000", "g-1")

        self.assertTrue(success)
        mock_http.delete.assert_called_once_with(
            "http://node1:8000/v1/group/", json={"groupId": "g-1"}, timeout=5.0,
        )

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_404_counts_as_success(self, _):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_http.delete.return_value = httpx.Response(404, request=httpx.Request("DELETE", "http://test"))

        _, success = await rollback_on_node(mock_http, "http://node1:8000", "g-1")

        self.assertTrue(success)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_retry_then_success(self, _):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        req = httpx.Request("DELETE", "http://test")
        mock_http.delete.side_effect = [
            httpx.ConnectError("Failed", request=req),
            httpx.Response(500, request=req),
            httpx.Response(500, request=req),
            httpx.ConnectError("Failed", request=req),
            httpx.Response(200, request=req),
        ]

        _, success = await rollback_on_node(mock_http, "http://node1:8000", "g-1")

        self.assertTrue(success)
        self.assertEqual(mock_http.delete.call_count, 5)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_fails_after_5_retries(self, mock_sleep):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        req = httpx.Request("DELETE", "http://test")
        mock_http.delete.side_effect = [httpx.ConnectError("Failed", request=req)] * 5

        _, success = await rollback_on_node(mock_http, "http://node1:8000", "g-1")

        self.assertFalse(success)
        self.assertEqual(mock_http.delete.call_count, 5)
        mock_sleep.assert_has_calls([call(0.5), call(1.0), call(2.0), call(4.0)])


class TestDeleteOnNode(IsolatedAsyncioTestCase):
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_success(self, _):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_http.delete.return_value = httpx.Response(200, request=httpx.Request("DELETE", "http://test"))

        node, success = await delete_on_node(mock_http, "http://node1:8000", "g-1")

        self.assertTrue(success)
        mock_http.delete.assert_called_once_with(
            "http://node1:8000/v1/group/", json={"groupId": "g-1"}, timeout=5.0,
        )

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_404_is_not_treated_as_success(self, _):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_http.delete.return_value = httpx.Response(404, request=httpx.Request("DELETE", "http://test"))

        _, success = await delete_on_node(mock_http, "http://node1:8000", "g-1")

        self.assertFalse(success)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_exponential_backoff_delays(self, mock_sleep):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        req = httpx.Request("DELETE", "http://test")
        mock_http.delete.side_effect = [httpx.ConnectError("Failed", request=req)] * 5

        await delete_on_node(mock_http, "http://node1:8000", "g-1")

        mock_sleep.assert_has_calls([call(0.5), call(1.0), call(2.0), call(4.0)])


class TestGetOnNode(IsolatedAsyncioTestCase):
    async def test_success(self):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_http.get.return_value = httpx.Response(
            200, json={"groupId": "g-1"}, request=httpx.Request("GET", "http://test"),
        )

        node, data = await get_on_node(mock_http, "http://node1:8000", "g-1")

        self.assertEqual(data, {"groupId": "g-1"})
        mock_http.get.assert_called_once_with("http://node1:8000/v1/group/g-1/", timeout=5.0)

    async def test_404_returns_none(self):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_http.get.return_value = httpx.Response(404, request=httpx.Request("GET", "http://test"))

        _, data = await get_on_node(mock_http, "http://node1:8000", "g-1")

        self.assertIsNone(data)

    async def test_connection_error_returns_none(self):
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_http.get.side_effect = httpx.ConnectError("Failed", request=httpx.Request("GET", "http://test"))

        _, data = await get_on_node(mock_http, "http://node1:8000", "g-1")

        self.assertIsNone(data)


class TestCreateGroup(IsolatedAsyncioTestCase):
    @patch("cluster_group_client.main.create_on_node")
    async def test_all_succeed(self, mock_create):
        mock_create.side_effect = [
            ("http://node1:8000", True),
            ("http://node2:8000", True),
        ]

        result = await create_group("g-1", NODES)

        self.assertTrue(result["success"])
        self.assertEqual(result["created_nodes"], {"http://node1:8000": True, "http://node2:8000": True})
        self.assertEqual(result["deleted_nodes"], {})

    @patch("cluster_group_client.main.rollback_on_node")
    @patch("cluster_group_client.main._check_group_exists")
    @patch("cluster_group_client.main.create_on_node")
    async def test_partial_failure_rolls_back(self, mock_create, mock_check, mock_rollback):
        mock_create.side_effect = [
            ("http://node1:8000", True),
            ("http://node2:8000", False),
        ]
        # Verification confirms group does NOT exist on failed node
        mock_check.return_value = False
        mock_rollback.return_value = ("http://node1:8000", True)

        result = await create_group("g-1", NODES)

        self.assertFalse(result["success"])
        self.assertEqual(result["created_nodes"], {"http://node1:8000": True, "http://node2:8000": False})
        self.assertEqual(result["deleted_nodes"], {"http://node1:8000": True})
        mock_rollback.assert_called_once()

    @patch("cluster_group_client.main.rollback_on_node")
    @patch("cluster_group_client.main._check_group_exists")
    @patch("cluster_group_client.main.create_on_node")
    async def test_all_fail_no_rollback(self, mock_create, mock_check, mock_rollback):
        mock_create.side_effect = [
            ("http://node1:8000", False),
            ("http://node2:8000", False),
        ]
        mock_check.return_value = False

        result = await create_group("g-1", NODES)

        self.assertFalse(result["success"])
        self.assertEqual(result["deleted_nodes"], {})
        mock_rollback.assert_not_called()

    @patch("cluster_group_client.main._check_group_exists")
    @patch("cluster_group_client.main.create_on_node")
    async def test_failed_node_verified_existing_becomes_success(self, mock_create, mock_check):
        """Node reports failure but GET confirms group exists (timeout ambiguity)."""
        mock_create.side_effect = [
            ("http://node1:8000", True),
            ("http://node2:8000", False),
        ]
        # Verification reveals the group actually exists on the "failed" node
        mock_check.return_value = True

        result = await create_group("g-1", NODES)

        self.assertTrue(result["success"])
        self.assertEqual(result["created_nodes"], {"http://node1:8000": True, "http://node2:8000": True})

    @patch("cluster_group_client.main.rollback_on_node")
    @patch("cluster_group_client.main._check_group_exists")
    @patch("cluster_group_client.main.create_on_node")
    async def test_verified_existing_node_is_rolled_back_if_others_still_fail(
        self, mock_create, mock_check, mock_rollback,
    ):
        """One failed node verified as existing, but another truly failed — all get rolled back."""
        nodes = ["http://node1:8000", "http://node2:8000", "http://node3:8000"]
        mock_create.side_effect = [
            ("http://node1:8000", True),
            ("http://node2:8000", False),
            ("http://node3:8000", False),
        ]
        # node2 actually has the group, node3 truly doesn't
        mock_check.side_effect = [True, False]
        mock_rollback.side_effect = [
            ("http://node1:8000", True),
            ("http://node2:8000", True),
        ]

        result = await create_group("g-1", nodes)

        self.assertFalse(result["success"])
        # node2 was upgraded to True after verification
        self.assertEqual(result["created_nodes"]["http://node2:8000"], True)
        self.assertEqual(result["created_nodes"]["http://node3:8000"], False)
        # Both node1 and node2 should be rolled back
        self.assertEqual(
            result["deleted_nodes"],
            {"http://node1:8000": True, "http://node2:8000": True},
        )

    async def test_no_nodes_raises(self):
        with self.assertRaises(ValueError):
            await create_group("g-1", [])


class TestDeleteGroup(IsolatedAsyncioTestCase):
    @patch("cluster_group_client.main.delete_on_node")
    async def test_all_succeed(self, mock_delete):
        mock_delete.side_effect = [
            ("http://node1:8000", True),
            ("http://node2:8000", True),
        ]

        result = await delete_group("g-1", NODES)

        self.assertTrue(result["success"])
        self.assertEqual(result["deleted_nodes"], {"http://node1:8000": True, "http://node2:8000": True})

    @patch("cluster_group_client.main.delete_on_node")
    async def test_partial_failure(self, mock_delete):
        mock_delete.side_effect = [
            ("http://node1:8000", True),
            ("http://node2:8000", False),
        ]

        result = await delete_group("g-1", NODES)

        self.assertFalse(result["success"])

    async def test_no_nodes_raises(self):
        with self.assertRaises(ValueError):
            await delete_group("g-1", [])


class TestGetGroup(IsolatedAsyncioTestCase):
    @patch("cluster_group_client.main.get_on_node")
    async def test_returns_results(self, mock_get):
        mock_get.side_effect = [
            ("http://node1:8000", {"groupId": "g-1"}),
            ("http://node2:8000", None),
        ]

        result = await get_group("g-1", NODES)

        self.assertEqual(result["http://node1:8000"], {"groupId": "g-1"})
        self.assertIsNone(result["http://node2:8000"])

    async def test_no_nodes_raises(self):
        with self.assertRaises(ValueError):
            await get_group("g-1", [])
