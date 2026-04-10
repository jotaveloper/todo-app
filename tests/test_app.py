import tempfile
import unittest
from datetime import date, timedelta
import io
import json
from pathlib import Path
import re

import main


class TodoAppTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        main.DB_PATH = Path(self.tmpdir.name) / "test_tasks.db"
        main.init_db()
        self.client = main.app.test_client()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _add_task(
        self,
        text,
        *,
        category="",
        priority="media",
        recurrence="",
        due_date="",
        notes="",
        follow_redirects=True,
    ):
        return self.client.post(
            "/add",
            data={
                "text": text,
                "category": category,
                "priority": priority,
                "recurrence": recurrence,
                "due_date": due_date,
                "notes": notes,
                "filter": "all",
                "q": "",
                "sort": "created_desc",
            },
            follow_redirects=follow_redirects,
        )

    def _tasks_section_html(self, html):
        match = re.search(
            r"<section class=\"tasks\">(.*?)</section>",
            html,
            flags=re.DOTALL,
        )
        return match.group(1) if match else ""

    def _task_texts(self, html):
        return re.findall(
            r"<span class=\"task-text\">\s*(.*?)\s*</span>",
            html,
            flags=re.DOTALL,
        )

    def test_crud_task_flow(self):
        add = self._add_task("Comprar leche", category="Casa", priority="alta")
        self.assertEqual(add.status_code, 200)
        self.assertIn("Comprar leche", add.data.decode("utf-8"))

        edit = self.client.post(
            "/edit/1",
            data={
                "text": "Comprar pan",
                "category": "Casa",
                "priority": "baja",
                "recurrence": "",
                "due_date": "",
                "filter": "all",
                "q": "",
                "sort": "created_desc",
            },
            follow_redirects=True,
        )
        self.assertEqual(edit.status_code, 200)
        self.assertIn("Comprar pan", edit.data.decode("utf-8"))

        toggle = self.client.post(
            "/toggle/1",
            data={"filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        self.assertEqual(toggle.status_code, 200)

        completed = self.client.get("/?filter=completed")
        self.assertIn("Comprar pan", completed.data.decode("utf-8"))

        delete = self.client.post(
            "/delete/1",
            data={"filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        self.assertEqual(delete.status_code, 200)
        task_texts = self._task_texts(delete.data.decode("utf-8"))
        self.assertNotIn("Comprar pan", task_texts)

    def test_empty_text_not_added(self):
        response = self._add_task("   ")
        self.assertEqual(response.status_code, 200)
        html = response.data.decode("utf-8")
        self.assertNotIn("<span class=\"task-text\">", html)

    def test_invalid_date_does_not_crash(self):
        response = self.client.post(
            "/add",
            data={
                "text": "Fecha inválida",
                "category": "",
                "priority": "media",
                "recurrence": "",
                "due_date": "2026-99-99",
                "filter": "all",
                "q": "",
                "sort": "created_desc",
            },
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("Fecha inválida", response.data.decode("utf-8"))

    def test_filters_search_sort(self):
        self._add_task("Alpha", category="Trabajo", priority="alta")
        self._add_task("Beta", category="Casa", priority="media")
        self._add_task("Gamma", category="Estudio", priority="baja")

        self.client.post(
            "/toggle/2",
            data={"filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )

        pending = self.client.get("/?filter=pending")
        pending_task_texts = self._task_texts(pending.data.decode("utf-8"))
        self.assertIn("Alpha", pending_task_texts)
        self.assertNotIn("Beta", pending_task_texts)

        completed = self.client.get("/?filter=completed")
        self.assertIn("Beta", completed.data.decode("utf-8"))

        search = self.client.get("/?filter=all&q=Estu")
        search_task_texts = self._task_texts(search.data.decode("utf-8"))
        self.assertIn("Gamma", search_task_texts)
        self.assertNotIn("Alpha", search_task_texts)

        sorted_priority = self.client.get("/?filter=all&q=&sort=priority").data.decode(
            "utf-8"
        )
        priority_order = self._task_texts(sorted_priority)
        self.assertLess(priority_order.index("Alpha"), priority_order.index("Beta"))
        self.assertLess(priority_order.index("Beta"), priority_order.index("Gamma"))

    def test_categories_persist_and_no_default_general(self):
        self.client.post(
            "/categories/add",
            data={"new_category": "Finanzas", "filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        self.client.post(
            "/categories/add",
            data={"new_category": "Salud", "filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        page = self.client.get("/")
        html = page.data.decode("utf-8")
        self.assertIn("value=\"Finanzas\"", html)
        self.assertIn("value=\"Salud\"", html)
        self.assertNotIn("value=\"General\"", html)

    def test_notes_flow_and_cascade_delete(self):
        self._add_task("Principal")
        self.client.post(
            "/notes/add/1",
            data={"note_text": "Nota 1", "filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        self.client.post(
            "/notes/toggle/1",
            data={"filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        page = self.client.get("/")
        self.assertIn("Nota 1", page.data.decode("utf-8"))

        self.client.post(
            "/delete/1",
            data={"filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        # Ensure no crash and task removed.
        cleaned = self.client.get("/")
        cleaned_task_texts = self._task_texts(cleaned.data.decode("utf-8"))
        self.assertNotIn("Principal", cleaned_task_texts)

    def test_recurring_task_rolls_due_date(self):
        today = date.today()
        self._add_task("Diaria", recurrence="daily", due_date=today.isoformat())
        self._add_task("Semanal", recurrence="weekly")
        self._add_task("Mensual", recurrence="monthly")

        self.client.post(
            "/toggle/1",
            data={"filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        self.client.post(
            "/toggle/2",
            data={"filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        self.client.post(
            "/toggle/3",
            data={"filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )

        with main.get_connection() as conn:
            rows = conn.execute(
                "SELECT id, completed, due_date FROM tasks ORDER BY id ASC"
            ).fetchall()
        self.assertEqual(rows[0]["completed"], 0)
        self.assertEqual(rows[0]["due_date"], (today + timedelta(days=1)).isoformat())
        self.assertEqual(rows[1]["completed"], 0)
        self.assertEqual(rows[1]["due_date"], (today + timedelta(days=7)).isoformat())
        self.assertEqual(rows[2]["completed"], 0)
        self.assertTrue(rows[2]["due_date"])

    def test_dashboard_and_metrics_render(self):
        self._add_task("A", category="Trabajo", priority="alta", due_date=(date.today() - timedelta(days=1)).isoformat())
        self._add_task("B", category="Trabajo", priority="media")
        self.client.post(
            "/toggle/2",
            data={"filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        self.client.post(
            "/notes/add/1",
            data={"note_text": "Nota A", "filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        home = self.client.get("/")
        html = home.data.decode("utf-8")
        self.assertIn("Métricas de productividad", html)
        self.assertIn("Completadas hoy:", html)
        self.assertIn("Por Prioridad", html)
        self.assertIn("Por Categoría", html)
        self.assertIn("Recordatorios", html)

    def test_export_returns_valid_json_payload(self):
        self._add_task("Respaldar datos", category="Admin", priority="alta")
        self.client.post(
            "/notes/add/1",
            data={"note_text": "Nota respaldo", "filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )

        response = self.client.get("/export")
        self.assertEqual(response.status_code, 200)
        self.assertIn("application/json", response.headers.get("Content-Type", ""))
        body = response.data.decode("utf-8")
        payload = json.loads(body)

        self.assertIn("tables", payload)
        self.assertIn("tasks", payload["tables"])
        self.assertIn("subtasks", payload["tables"])
        self.assertIn("categories", payload["tables"])
        self.assertIn("activity_log", payload["tables"])
        self.assertGreaterEqual(len(payload["tables"]["tasks"]), 1)

    def test_import_restores_tasks(self):
        self._add_task("Original", category="Trabajo", priority="media", due_date=date.today().isoformat())
        exported = self.client.get("/export")
        payload = exported.data.decode("utf-8")

        self.client.post(
            "/delete/1",
            data={"filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        page_after_delete = self.client.get("/")
        self.assertNotIn("Original", self._task_texts(page_after_delete.data.decode("utf-8")))

        import_response = self.client.post(
            "/import",
            data={
                "import_file": (io.BytesIO(payload.encode("utf-8")), "backup.json"),
                "filter": "all",
                "q": "",
                "sort": "created_desc",
            },
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        html = import_response.data.decode("utf-8")
        self.assertEqual(import_response.status_code, 200)
        self.assertIn("Importación completada correctamente.", html)
        self.assertIn("Original", self._task_texts(html))

    def test_import_invalid_json_shows_error(self):
        response = self.client.post(
            "/import",
            data={
                "import_file": (io.BytesIO(b"{invalid_json"), "bad.json"),
                "filter": "all",
                "q": "",
                "sort": "created_desc",
            },
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("Importación fallida:", response.data.decode("utf-8"))

    def test_reorder_updates_persistent_position(self):
        self._add_task("Tarea 1")
        self._add_task("Tarea 2")
        self._add_task("Tarea 3")

        reorder = self.client.post(
            "/reorder",
            json={"ordered_ids": [2, 3, 1]},
        )
        self.assertEqual(reorder.status_code, 200)

        page = self.client.get("/?sort=created_desc")
        order = self._task_texts(page.data.decode("utf-8"))
        self.assertEqual(order[:3], ["Tarea 1", "Tarea 3", "Tarea 2"])

        with main.get_connection() as conn:
            rows = conn.execute(
                "SELECT id, position FROM tasks ORDER BY position ASC"
            ).fetchall()
        self.assertEqual([row["id"] for row in rows], [2, 3, 1])

    def test_reorder_invalid_payload_returns_400(self):
        self._add_task("A")
        response = self.client.post("/reorder", json={"ordered_ids": ["x"]})
        self.assertEqual(response.status_code, 400)

    def test_calendar_view_renders_and_uses_due_dates(self):
        today = date.today().replace(day=15).isoformat()
        self._add_task("Calendario tarea", due_date=today, priority="alta")
        page = self.client.get("/?nav=calendar")
        html = page.data.decode("utf-8")
        self.assertEqual(page.status_code, 200)
        self.assertIn("calendar-grid", html)
        self.assertIn("Calendario tarea", html)

    def test_calendar_drag_move_updates_due_date(self):
        self._add_task("Mover fecha", due_date=date.today().isoformat())
        target_date = (date.today() + timedelta(days=2)).isoformat()
        response = self.client.post(
            "/calendar/move",
            json={"task_id": 1, "new_date": target_date},
        )
        self.assertEqual(response.status_code, 200)
        with main.get_connection() as conn:
            row = conn.execute("SELECT due_date FROM tasks WHERE id = 1").fetchone()
        self.assertEqual(row["due_date"], target_date)

    def test_calendar_contains_task_detail_payload_with_notes(self):
        self._add_task("Detalle calendario", category="Trabajo", priority="alta", due_date=date.today().isoformat())
        self.client.post(
            "/notes/add/1",
            data={"note_text": "Nota interna", "filter": "all", "q": "", "sort": "created_desc", "nav": "calendar"},
            follow_redirects=True,
        )
        page = self.client.get("/?nav=calendar")
        html = page.data.decode("utf-8")
        self.assertIn("calendar-task-details", html)
        self.assertIn("Detalle calendario", html)
        self.assertIn("Nota interna", html)

    def test_stats_view_renders_with_chart_payload(self):
        self._add_task("Tarea stats", category="Trabajo", priority="alta")
        page = self.client.get("/?nav=stats&stats_range=30")
        html = page.data.decode("utf-8")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Vista de estadísticas", html)
        self.assertIn("stats-chart-data", html)
        self.assertIn("cdn.jsdelivr.net/npm/chart.js", html)

    def test_sidebar_links_have_icons(self):
        page = self.client.get("/")
        html = page.data.decode("utf-8")
        self.assertIn("sidebar-link-icon", html)
        self.assertIn("Estadísticas", html)

    def test_global_search_input_is_rendered(self):
        page = self.client.get("/?nav=tasks")
        html = page.data.decode("utf-8")
        self.assertIn("id=\"global_q\"", html)
        self.assertIn("Buscar categoría...", html)
        self.assertIn("id=\"global_date_q\"", html)

    def test_search_matches_category_and_date_filter(self):
        due_date = date.today().isoformat()
        self._add_task("Revisar backlog", category="Marketing", priority="alta", due_date=due_date)
        self._add_task("Preparar informe", category="Finanzas", priority="media")

        by_category = self.client.get("/?q=Market")
        self.assertIn("Revisar backlog", by_category.data.decode("utf-8"))

        by_non_category = self.client.get("/?q=alta")
        non_category_task_texts = self._task_texts(by_non_category.data.decode("utf-8"))
        self.assertNotIn("Revisar backlog", non_category_task_texts)

        by_date = self.client.get(f"/?date_q={due_date}")
        self.assertIn("Revisar backlog", by_date.data.decode("utf-8"))

    def test_notes_create_render_and_edit_flow(self):
        response = self._add_task("Tarea con nota", notes="Nota inicial")
        html = response.data.decode("utf-8")
        self.assertIn("Tarea con nota", html)
        self.assertIn("Nota: Nota inicial", html)

        edit = self.client.post(
            "/edit/1",
            data={
                "text": "Tarea con nota",
                "notes": "Nota actualizada",
                "category": "",
                "priority": "media",
                "recurrence": "",
                "due_date": "",
                "filter": "all",
                "q": "",
                "sort": "created_desc",
            },
            follow_redirects=True,
        )
        self.assertEqual(edit.status_code, 200)
        self.assertIn("Nota: Nota actualizada", edit.data.decode("utf-8"))

    def test_notes_remains_separate_from_note_items(self):
        self._add_task("Tarea principal", notes="Nota propia")
        self.client.post(
            "/notes/add/1",
            data={"note_text": "Nota item A", "filter": "all", "q": "", "sort": "created_desc"},
            follow_redirects=True,
        )
        page = self.client.get("/")
        html = page.data.decode("utf-8")
        self.assertIn("Nota: Nota propia", html)
        self.assertIn("Nota item A", html)


if __name__ == "__main__":
    unittest.main()
