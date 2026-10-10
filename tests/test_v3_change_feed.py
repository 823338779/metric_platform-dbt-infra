"""变化流空页、重放和过滤游标不能遗漏已提交事实。"""

from tests.test_publication_storage import store as store
from tests.test_v3_build_acceptance import CALLER, request, service


def test_change_feed_replays_acceptance_and_empty_page_keeps_cursor(store):
    from dbt_metricflow_service.storage.changes import ChangeStore

    app = service(store)
    build = app.submit(request(), CALLER)
    changes = ChangeStore(store.db)
    cursor, seen = None, []
    while True:
        page = changes.read(cursor, 200)
        seen.extend(page.items)
        if not page.items:
            assert page.next_cursor == cursor
            break
        cursor = page.next_cursor
    assert any(
        item.object_id == str(build.build_id) and item.summary["buildId"] == str(build.build_id) for item in seen
    )
    assert [item.sequence for item in seen] == sorted({item.sequence for item in seen})


def test_rolled_back_fact_does_not_advance_cursor(store):
    from dbt_metricflow_service.storage.changes import ChangeStore

    changes = ChangeStore(store.db)
    with store.db.transaction() as connection:
        before = connection.exec_driver_sql("SELECT value FROM engine_change_counter").scalar_one()
    try:
        with store.db.transaction() as connection:
            connection.exec_driver_sql("UPDATE engine_change_counter SET value=value+1")
            raise ValueError("rollback")
    except ValueError:
        pass
    assert changes.read(str(before), 200).next_cursor == str(before)


def test_change_cursor_cannot_skip_uncommitted_update(store):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    from dbt_metricflow_service.storage.changes import ChangeStore

    app = service(store)
    first = app.submit(request(), CALLER)
    second = app.submit(request(), CALLER)
    started = Event()

    def update_second():
        started.set()
        app.cancel(second.build_id, CALLER)

    with ThreadPoolExecutor(max_workers=1) as pool:
        with store.db.transaction() as connection:
            before = connection.exec_driver_sql("SELECT value FROM engine_change_counter FOR UPDATE").scalar_one()
            connection.exec_driver_sql(
                "UPDATE engine_build SET phase='FIRST',version=version+1 WHERE build_id=%s", (str(first.build_id),)
            )
            pending = pool.submit(update_second)
            assert started.wait(2)
            assert ChangeStore(store.db).read(str(before)).items == []
            assert not pending.done()
        pending.result(timeout=5)
    changes = ChangeStore(store.db).read(str(before), 200)
    assert changes.items[0].object_id == str(first.build_id)
    assert str(second.build_id) in {item.object_id for item in changes.items[1:]}
