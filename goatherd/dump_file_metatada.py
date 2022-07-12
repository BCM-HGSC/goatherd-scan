from multiprocessing import Process, Queue, set_start_method
from os import getpid
from pathlib import Path
from queue import Empty
from sys import stderr, stdout
from typing import Union

from .local_file_metadata import compute_md5, LocalFileMetadata, scan


def run(start_dir_path: Path) -> None:
    set_start_method("spawn")
    engine = TaskEngine(run_worker, handle_results, 4)
    engine.start()
    for r in scan(start_dir_path):
        engine.post_task(r)
        # r.set_md5_from_contents()
    engine.finish()


def run_worker(worker_id, worker_tasks: Queue, worker_results: Queue) -> None:
    pid = getpid()
    err(worker_id, pid, "start")
    while True:
        err(worker_id, pid, "get")
        file_path: Path = worker_tasks.get()
        assert isinstance(file_path, Path)
        err(worker_id, pid, file_path, "value")
        md5 = compute_md5(file_path)
        worker_results.put((file_path, md5))


WORK_END = "WORK END"  # sentinel object for task_defs Queue


def handle_results(worker_results: Queue, task_defs: Queue):
    err("results start")
    print(*LocalFileMetadata.field_names(), sep="\t")
    stdout.flush()
    remaining_tasks: dict[Path, LocalFileMetadata] = dict()
    task_defs_closed = False
    while remaining_tasks or not task_defs_closed:
        if not task_defs_closed:
            for task_def in iterate_quick(task_defs):
                if not task_def == WORK_END:
                    assert isinstance(task_def, LocalFileMetadata), task_def
                    file_path = task_def.path
                    assert file_path not in remaining_tasks
                    remaining_tasks[file_path] = task_def
                else:
                    err("Got WORK_END")
                    task_defs_closed = True
        err("fetching results")
        for result in iterate_quick(worker_results):
            err("result", *result)
            file_path, md5 = result
            assert isinstance(file_path, Path), file_path
            assert isinstance(md5, str), md5
            assert file_path in remaining_tasks, (result, remaining_tasks)
            metadata = remaining_tasks.pop(file_path)
            metadata.md5 = md5
            print(*metadata.astuple, sep="\t")
    assert not remaining_tasks, remaining_tasks


def iterate_quick(q: Queue):
    while True:
        try:
            yield q.get(True, 0.01)
        except Empty:
            return


class TaskEngine:
    def __init__(self, worker_function, results_handler, num_workers: int = 2):
        self.worker_tasks = Queue()
        self.worker_results = Queue()
        self.task_defs = Queue()
        self.workers = [
            Process(
                target=worker_function,
                args=(
                    worker_id,
                    self.worker_tasks,
                    self.worker_results,
                ),
            )
            for worker_id in range(num_workers)
        ]
        self.result_process = Process(
            target=results_handler, args=(self.worker_results, self.task_defs)
        )

    def post_task(self, metadata: LocalFileMetadata):
        # err("posting", metadata.path)
        self.worker_tasks.put(metadata.path)
        self.task_defs.put(metadata)

    def start(self):
        for worker in self.workers:
            worker.start()
        self.result_process.start()

    def finish(self):
        """Wait for self.result_process to exit, then kill all workers and exit."""
        self.task_defs.put(WORK_END)
        self.worker_tasks.close()
        self.task_defs.close()
        self.result_process.join()
        for worker in self.workers:
            err("terminating", worker.name)
            worker.terminate()


def err(*args, **kwargs) -> None:
    return
    print(*args, file=stderr, **kwargs)

# err("finished loading", __file__)
